import logging

import geopandas as gpd
import pandas as pd

from ..config import (
    THEME_DRIVER,
    THEME_SUFFIX,
    TRANSFORM_SUFFIX,
    WORKING_THEME_DIR,
    WORKING_TRANSFORM_DIR,
    get_theme_by_name,
)
from ..log import log_context
from ..schema_check import RFC3339_STRING, schema_dtypes
from .transform import read_transform

logger = logging.getLogger("kart_import")

LIFECYCLE_COLUMNS = ("created_at", "updated_at")
"""Pipeline-managed timestamps. Normalised here rather than left to the schema so a theme with
no schema still emits the same RFC 3339 text as one with a schema."""


_NULLABLE_PREFIXES = {"i": "Int", "u": "UInt", "f": "Float"}


def _nullable(dtype) -> str | None:
    """The nullable equivalent of a numpy dtype (``int32`` -> ``Int32``), or None if there
    isn't one worth having.

    Nullable throughout because a numpy int column that has to hold a NULL degrades to object
    or float. None covers object -- two sources disagreeing on a column's type -- where
    there's nothing safe to infer and the schema has to settle it.
    """
    if isinstance(dtype, pd.api.extensions.ExtensionDtype):
        return str(dtype)
    if dtype.kind == "b":
        return "boolean"
    prefix = _NULLABLE_PREFIXES.get(dtype.kind)
    return f"{prefix}{dtype.itemsize * 8}" if prefix else None


def _cast(series: pd.Series, target: str) -> pd.Series:
    """Cast to `target`, tolerating a column that is NULL for every row.

    Such a column carries no values to convert, and pandas refuses to cast its float64 NaN
    (how an all-NULL column survives the parquet round trip) to a nullable integer. Rebuild it
    as typed NULLs instead of pushing the NaN through a cast that can only fail.
    """
    dtype = pd.api.types.pandas_dtype(target)
    if series.isna().all():
        return pd.Series(pd.NA, index=series.index, dtype=dtype)
    return series.astype(dtype)


def unify_dtypes(gdfs: list[gpd.GeoDataFrame]) -> None:
    """Give every frame one dtype per column, in place, so `concat` can't fall back to object.

    A dataset mapped ``col: null`` contributes an all-NULL object column. Concatenated with a
    sibling's int column pandas widens the result to object, and pyogrio writes an object column
    as OFTString whatever the driver. This means an integer would reach kart as text.
    Take the dtype from the datasets that do carry values and push it onto the ones that don't.

    This runs without consulting a schema, so it holds for local dev where none exists.
    """
    columns = list(dict.fromkeys(col for gdf in gdfs for col in gdf.columns))
    for col in columns:
        if any(col == gdf.geometry.name for gdf in gdfs):
            continue
        populated = [gdf[col].dtype for gdf in gdfs if col in gdf.columns and gdf[col].notna().any()]
        if not populated:
            continue
        common = pd.concat([pd.Series([], dtype=dtype) for dtype in populated]).dtype
        target = _nullable(common)
        if target is None:
            # Genuinely mixed (e.g. text in one source, int in another)
            # No safe common type, leave it for `coerce_dtypes` to settle against the schema.
            continue
        for gdf in gdfs:
            if col in gdf.columns and gdf[col].dtype != target:
                gdf[col] = _cast(gdf[col], target)


def _to_rfc3339(series: pd.Series) -> pd.Series:
    """Normalise a timestamp column to RFC 3339 UTC text (``2015-11-19T02:26:49Z``).

    Parsing and reformatting rather than passing the source text through: `format: date-time`
    demands an offset, and a bare `astype("string")` over a datetime column yields a
    space-separated form that fails it. Second precision, which is all a git commit time
    (the source of these) carries.
    """
    parsed = pd.to_datetime(series, utc=True, format="ISO8601")
    return parsed.dt.strftime("%Y-%m-%dT%H:%M:%SZ").astype("string")


def _carries(dtype, target: str) -> bool:
    """Whether `dtype` already conveys the schema's type, so the cast can be skipped.

    Keeps a narrower nullable dtype that `unify_dtypes` derived from the sources (Int32 for an
    ``integer``) rather than widening it for no reason.
    """
    if not isinstance(dtype, pd.api.extensions.ExtensionDtype):
        return False
    if target == "Int64":
        return pd.api.types.is_integer_dtype(dtype)
    if target == "Float64":
        return pd.api.types.is_float_dtype(dtype)
    return str(dtype) == target


def coerce_dtypes(merged: gpd.GeoDataFrame, theme_name: str) -> gpd.GeoDataFrame:
    """Force the merged frame onto the dtypes the theme's JSON schema declares.

    `unify_dtypes` only preserves what the sources happened to carry; this asserts the target
    contract on top, so a source handing us a float where the schema says integer is corrected
    here (or fails loudly) rather than silently becoming text downstream. Columns the schema
    doesn't describe, and themes with no schema at all, keep their inferred dtype.
    """
    targets = {col: RFC3339_STRING for col in LIFECYCLE_COLUMNS if col in merged.columns}
    targets.update({col: dtype for col, dtype in schema_dtypes(theme_name).items() if col in merged.columns})

    for col, target in targets.items():
        series = merged[col]
        try:
            if target == RFC3339_STRING:
                merged[col] = _cast(series, "string") if series.isna().all() else _to_rfc3339(series)
            elif not _carries(series.dtype, target):
                merged[col] = _cast(series, target)
        except (TypeError, ValueError) as e:
            raise ValueError(f"{theme_name}.{col}: cannot cast {series.dtype} to {target}: {e}") from e

    return merged


def merge_theme_release(theme_name: str, release_id: int):
    theme = get_theme_by_name(theme_name)

    logger.info(f"Merging theme: {theme.name} for release {release_id}")

    release_dir = WORKING_THEME_DIR / f"release_{release_id}"
    release_dir.mkdir(parents=True, exist_ok=True)
    output_file = release_dir / f"{theme.name}{THEME_SUFFIX}"
    gdfs = []

    if output_file.exists():
        output_file.unlink()

    missing = []
    for dataset in theme.datasets:
        transform_path = WORKING_TRANSFORM_DIR / f"release_{release_id}" / f"{dataset.name}{TRANSFORM_SUFFIX}"
        if not transform_path.exists():
            logger.warning(
                f"Transformed file not found: {transform_path}. Skipping.", extra={"source_dataset": dataset.name}
            )
            missing.append(dataset.name)
            continue
        gdf = read_transform(transform_path)
        if gdf.empty:
            logger.info(f"{dataset.name} (release {release_id}) is empty. Skipping.")
            continue

        logger.info(f"{dataset.name} (release {release_id}): {len(gdf)} features")
        gdfs.append(gdf)

    if missing:
        raise FileNotFoundError(
            f"{theme.name} release {release_id}: no transform output for {', '.join(missing)}. Run transform first."
        )

    if not gdfs:
        logger.warning(f"No data found for theme {theme.name} release {release_id}.")
        return

    unify_dtypes(gdfs)
    merged = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)
    merged = coerce_dtypes(merged, theme.name)

    # Stable sorting to keep row order predictable (Note: FlatGeobuf writes in spatial-index order)
    if "id" in merged.columns:
        merged = merged.sort_values(by=["id"]).reset_index(drop=True)

    logger.info(f"Writing {len(merged)} total features into {output_file}")

    # Explicitly remove fid if it exists and ensure index is not written
    if "fid" in merged.columns:
        merged = merged.drop(columns=["fid"])

    merged.to_file(output_file, driver=THEME_DRIVER, index=False)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python -m kart_import.assets.theme <theme_name> <release_id>")
        sys.exit(1)
    with log_context(action="theme", theme=sys.argv[1], release=int(sys.argv[2])):
        merge_theme_release(sys.argv[1], int(sys.argv[2]))
