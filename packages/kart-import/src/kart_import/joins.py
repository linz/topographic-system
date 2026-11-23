import functools
import logging
from dataclasses import dataclass

import geopandas as gpd
import pandas as pd
import shapely

from .config import (
    WORKING_LOOKUP_DIR,
    Join,
    Lookup,
    ThemeDataset,
    get_lookup_by_name,
)
from .export_paths import export_entry_dir, load_releases_map, releases_map_file, theme_hash_file

logger = logging.getLogger("kart_import")


def _join_key_category(series: pd.Series) -> str:
    """Coarse type bucket used to decide whether two join keys are compatible."""
    dtype = series.dtype
    if pd.api.types.is_bool_dtype(dtype):
        return "bool"
    if pd.api.types.is_integer_dtype(dtype):
        return "integer"
    if pd.api.types.is_float_dtype(dtype):
        return "float"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "datetime"
    if pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype):
        return "string"
    return dtype.name


def _require_compatible_join_keys(
    left: pd.Series, right: pd.Series, *, lookup_name: str, left_on: str, lookup_key: str
) -> None:
    """Raise unless two join keys can be safely merged.

    Keys must share a coarse type (int<->int, str<->str, ...); differing types are a config/data
    error we surface rather than silently coerce. An empty key column (a release or lookup with no
    rows) is compatible: there is nothing to join, so no type can clash.
    """
    if left.empty or right.empty:
        return
    left_cat = _join_key_category(left)
    right_cat = _join_key_category(right)
    if left_cat != right_cat:
        raise TypeError(
            f"join key type mismatch for lookup {lookup_name!r}: "
            f"left '{left_on}' is {left.dtype} ({left_cat}), "
            f"right '{lookup_key}' is {right.dtype} ({right_cat}); "
            f"align the column types before joining"
        )


@functools.cache
def _resolve_lookup_version(lookup_name: str, release_id: int) -> str | None:
    """Which prepared file of the lookup this release joins against, by its name: the source
    commit as-of this release, or for a theme lookup the SHA-256 of that release's theme build.
    None if the release predates the lookup's history.

    Read from the release map `export_lookup` persists (see `export_paths.write_releases_map`)
    rather than a live `git log` against the clone, so transform has no dependency on the clone
    being present.

    Memoized on (lookup_name, release_id): a single transform process resolves the same pair
    repeatedly (once per release while fingerprinting in find_canonical_release, again when it
    recurses into the canonical release, and again in apply_joins).
    """
    lookup = get_lookup_by_name(lookup_name)
    if lookup.theme is not None:
        hash_file = theme_hash_file(lookup.theme, release_id)
        if not hash_file.exists():
            raise FileNotFoundError(
                f"lookup {lookup_name!r} reads theme {lookup.theme!r}, but its release {release_id} "
                f"hasn't been built: {hash_file}"
            )
        return hash_file.read_text().strip()

    export_dir = export_entry_dir(lookup_name)
    releases_file = releases_map_file(export_dir, lookup_name)
    if not releases_file.exists():
        raise FileNotFoundError(
            f"lookup {lookup_name!r} has no release map at {releases_file}; export it before transform"
        )

    releases_map = load_releases_map(export_dir, lookup_name)
    if release_id not in releases_map:
        return None

    commit = releases_map[release_id]["commit"]
    if commit is None and not any(info["commit"] for info in releases_map.values()):
        raise FileNotFoundError(f"lookup {lookup_name!r} at {releases_file} has no resolvable commits; re-export it")
    return commit


def join_fingerprint(td: ThemeDataset, release_id: int) -> tuple[tuple[str, str | None], ...]:
    """The lookup version (commit, or theme hash) each join resolves to for this release."""
    return tuple(sorted((join.lookup, _resolve_lookup_version(join.lookup, release_id)) for join in td.joins))


@dataclass
class _JoinPlan:
    """A validated, ready-to-apply join. `frame` is None when this release has no lookup commit.
    The columns are still added, but null-filled."""

    join: Join
    lookup: Lookup
    wanted: list[str]
    qualified: dict[str, str]  # source column -> namespaced output column
    frame: pd.DataFrame | gpd.GeoDataFrame | None


def _plan_joins(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> list[_JoinPlan]:
    """Resolve, load and type-check every join before any merge runs, so a mismatch fails fast and
    atomically rather than after some lookups have already been joined onto the frame."""
    plans: list[_JoinPlan] = []
    for join in td.joins:
        lookup = get_lookup_by_name(join.lookup)
        wanted = list(lookup.columns) if join.columns is None else join.columns
        # Namespace by lookup name so columns can't clash across lookups or with the source.
        qualified = {col: f"{lookup.name}.{col}" for col in wanted}

        version = _resolve_lookup_version(join.lookup, release_id)
        if version is None:
            plans.append(_JoinPlan(join, lookup, wanted, qualified, None))
            continue

        lookup_file = WORKING_LOOKUP_DIR / join.lookup / f"{version}.parquet"
        if not lookup_file.exists():
            raise FileNotFoundError(
                f"prepared lookup {join.lookup!r} missing for {version=} ({release_id=}): {lookup_file}"
            )

        if join.predicate:
            spatial = gpd.read_parquet(lookup_file)
            if spatial.crs is None:
                raise ValueError(f"spatial lookup {join.lookup!r} has no projection: {lookup_file}")
            plans.append(_JoinPlan(join, lookup, wanted, qualified, spatial))
            continue

        if join.left_on not in gdf.columns:
            raise KeyError(f"join left_on '{join.left_on}' not found in {td.name} source columns")
        lookup_data = pd.read_parquet(lookup_file)
        if lookup.key not in lookup_data.columns:
            raise KeyError(f"lookup key '{lookup.key}' not found in lookup {join.lookup!r} columns")
        _require_compatible_join_keys(
            gdf[join.left_on],
            lookup_data[lookup.key],
            lookup_name=join.lookup,
            left_on=join.left_on,
            lookup_key=lookup.key,
        )
        plans.append(_JoinPlan(join, lookup, wanted, qualified, lookup_data))
    return plans


def chop(gdf: gpd.GeoDataFrame, stride: int) -> gpd.GeoDataFrame:
    """Split linework into pieces of at most `stride` segments."""
    gdf = gdf.explode(index_parts=False).reset_index(drop=True)

    owner, pieces = [], []
    for index, geometry in enumerate(gdf.geometry.to_numpy()):
        coords = shapely.get_coordinates(geometry)
        for start in range(0, len(coords) - 1, stride):
            owner.append(index)
            pieces.append(shapely.LineString(coords[start : start + stride + 1]))

    attrs = gdf.drop(columns=gdf.geometry.name).iloc[owner].reset_index(drop=True)
    return gpd.GeoDataFrame(attrs, geometry=pieces, crs=gdf.crs)


def _spatial_join(gdf: gpd.GeoDataFrame, plan: _JoinPlan, dataset_name: str) -> gpd.GeoDataFrame:
    """Attach a spatial lookup's columns by geometry."""
    join = plan.join
    assert isinstance(plan.frame, gpd.GeoDataFrame)
    right = gpd.GeoDataFrame(plan.frame[[*plan.wanted, plan.frame.geometry.name]]).rename(columns=plan.qualified)

    left = gdf.reset_index(drop=True)
    crs = left.crs
    assert crs is not None
    if right.crs != crs:
        right = right.to_crs(crs)

    if join.chop:
        right = chop(right, join.chop)

    if join.predicate == "nearest":
        if join.max_distance is not None and crs.is_geographic:
            raise ValueError(
                f"{dataset_name}: join on '{join.lookup}' sets max_distance={join.max_distance}, but the "
                f"source is in geographic CRS {crs.to_string()} where that is degrees, not metres"
            )
        merged = gpd.sjoin_nearest(left, right, how="left", max_distance=join.max_distance)
    else:
        merged = gpd.sjoin(left, right, how="left", predicate=join.predicate)

    merged = merged.sort_index(kind="stable")
    duplicated = merged.index.duplicated(keep="first")
    if duplicated.any():
        logger.warning(
            "spatial join matched multiple lookup features; kept first",
            extra={"dataset": dataset_name, "lookup": join.lookup, "extra_matches": int(duplicated.sum())},
        )
        merged = merged[~duplicated]

    out = left.copy()
    for column in plan.qualified.values():
        out[column] = merged[column]
    return out


def apply_joins(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Join each configured lookup's columns onto the source frame, by key (`left_on`) or by
    geometry (`predicate`). All joins are validated up front, so a key-type mismatch on any of
    them fails before the frame is mutated."""
    for plan in _plan_joins(gdf, td, release_id):
        qualified = plan.qualified
        if plan.frame is None:
            logger.info(
                "no lookup for this release; filling join columns null",
                extra={
                    "dataset": td.name,
                    "lookup": plan.join.lookup,
                    "release": release_id,
                    "columns": list(qualified.values()),
                },
            )
            gdf = gdf.copy()
            for col in qualified.values():
                gdf[col] = pd.NA
            continue

        if plan.join.predicate:
            gdf = _spatial_join(gdf, plan, td.name)
        else:
            key = plan.lookup.key
            right = plan.frame[[key, *plan.wanted]].rename(columns=qualified)
            gdf = gdf.copy()
            merged = gdf.merge(right, left_on=plan.join.left_on, right_on=key, how="left")
            if key in merged.columns and key != plan.join.left_on:
                merged = merged.drop(columns=key)
            gdf = gpd.GeoDataFrame(merged, geometry=gdf.geometry.name, crs=gdf.crs)
        matched = int(gdf[qualified[plan.wanted[0]]].notna().sum()) if plan.wanted else 0
        logger.info(
            "apply_join",
            extra={
                "dataset": td.name,
                "lookup": plan.join.lookup,
                "columns": list(qualified.values()),
                "matched_rows": matched,
            },
        )
    return gdf
