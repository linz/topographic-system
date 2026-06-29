import logging

import geopandas as gpd
import pandas as pd

from .config import (
    SOURCE_DIR,
    WORKING_LOOKUP_DIR,
    ThemeDataset,
    get_lookup_by_name,
    get_releases,
)
from .git.release import get_release_commit

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


def _resolve_lookup_commit(lookup_name: str, release_id: int) -> str | None:
    """The lookup commit as-of this release, or None if the release predates the lookup's history."""
    release = next((r for r in get_releases() if r.id == release_id), None)
    if release is None:
        return None
    repo_dir = SOURCE_DIR / lookup_name
    if not repo_dir.exists():
        raise FileNotFoundError(
            f"lookup {lookup_name!r} source repo not found at {repo_dir}; clone+export the lookup before transform"
        )
    res = get_release_commit(repo_dir, release.until)
    if res is not None:
        return res[0]

    if get_release_commit(repo_dir, None) is None:  # check if missing release _or_ genuine `None` value
        raise FileNotFoundError(
            f"lookup {lookup_name!r} at {repo_dir} has no resolvable commits; re-clone/export the lookup"
        )
    return None


def join_fingerprint(td: ThemeDataset, release_id: int) -> tuple[tuple[str, str | None], ...]:
    """The lookup commit each join resolves to for this release."""
    return tuple(sorted((join.lookup, _resolve_lookup_commit(join.lookup, release_id)) for join in td.joins))


def validate_join_key_types(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> None:
    """Pre-flight every join's key types before any merge runs, so a mismatch fails fast and
    atomically rather than after some lookups have already been joined onto the frame.
    """
    for join in td.joins:
        if join.left_on not in gdf.columns:
            raise KeyError(f"join left_on '{join.left_on}' not found in {td.name} source columns")
        commit = _resolve_lookup_commit(join.lookup, release_id)
        if commit is None:
            continue
        lookup = get_lookup_by_name(join.lookup)
        lookup_file = WORKING_LOOKUP_DIR / join.lookup / f"{commit}.parquet"
        if not lookup_file.exists():
            raise FileNotFoundError(
                f"prepared lookup {join.lookup!r} missing for {commit=} ({release_id=}): {lookup_file}"
            )
        key = pd.read_parquet(lookup_file, columns=[lookup.key])[lookup.key]
        _require_compatible_join_keys(
            gdf[join.left_on], key, lookup_name=join.lookup, left_on=join.left_on, lookup_key=lookup.key
        )


def apply_joins(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Left-join each configured lookup's columns onto the source frame by key."""
    for join in td.joins:
        lookup = get_lookup_by_name(join.lookup)
        wanted = list(lookup.columns) if join.columns is None else join.columns
        # Namespace by lookup name so columns can't clash across lookups or with the source.
        qualified = {col: f"{lookup.name}.{col}" for col in wanted}

        commit = _resolve_lookup_commit(join.lookup, release_id)
        if commit is None:
            logger.info(
                "no lookup for this release; filling join columns null",
                extra={
                    "dataset": td.name,
                    "lookup": join.lookup,
                    "release": release_id,
                    "columns": list(qualified.values()),
                },
            )
            gdf = gdf.copy()
            for col in qualified.values():
                gdf[col] = pd.NA
            continue

        lookup_file = WORKING_LOOKUP_DIR / join.lookup / f"{commit}.parquet"
        if not lookup_file.exists():
            raise FileNotFoundError(
                f"prepared lookup {join.lookup!r} missing for {commit=} ({release_id=}): {lookup_file}"
            )
        if join.left_on not in gdf.columns:
            raise KeyError(f"join left_on '{join.left_on}' not found in {td.name} source columns")

        lk = pd.read_parquet(lookup_file)
        if lookup.key not in lk.columns:
            raise KeyError(f"lookup key '{lookup.key}' not found in lookup {join.lookup!r} columns")

        _require_compatible_join_keys(
            gdf[join.left_on], lk[lookup.key], lookup_name=join.lookup, left_on=join.left_on, lookup_key=lookup.key
        )

        right = lk[[lookup.key, *wanted]].rename(columns=qualified)
        gdf = gdf.copy()
        merged = gdf.merge(right, left_on=join.left_on, right_on=lookup.key, how="left")
        if lookup.key in merged.columns and lookup.key != join.left_on:
            merged = merged.drop(columns=lookup.key)
        gdf = gpd.GeoDataFrame(merged, geometry=gdf.geometry.name, crs=gdf.crs)
        matched = int(gdf[qualified[wanted[0]]].notna().sum()) if wanted else 0
        logger.info(
            "apply_join",
            extra={
                "dataset": td.name,
                "lookup": join.lookup,
                "columns": list(qualified.values()),
                "matched_rows": matched,
            },
        )
    return gdf
