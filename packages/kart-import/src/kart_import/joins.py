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


def _normalise_join_key(series: pd.Series) -> pd.Series:
    """Canonicalise a join key so int/float/string forms match (e.g. 5.0 == 5 == '5')."""
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().any():
        return numeric.astype("Int64").astype(
            "string"
        )  # FIXME: This will clobber leading-zero string keys. Remove / update if needed.
    return series.astype("string")


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
        right = lk[[lookup.key, *wanted]].copy()
        right["__join_key__"] = _normalise_join_key(right[lookup.key])
        right = right.drop(columns=[lookup.key]).rename(columns=qualified)

        gdf = gdf.copy()
        gdf["__join_key__"] = _normalise_join_key(gdf[join.left_on])
        merged = gdf.merge(right, on="__join_key__", how="left").drop(columns="__join_key__")
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
