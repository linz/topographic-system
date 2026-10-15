import hashlib
import logging
import os
from pathlib import Path

import geopandas as gpd
import pandas as pd

from ..config import (
    THEME_DRIVER,
    THEME_SUFFIX,
    TRANSFORM_SUFFIX,
    WORKING_THEME_DIR,
    WORKING_TRANSFORM_DIR,
    Release,
    Theme,
    get_releases,
    get_theme_by_name,
)
from ..log import log_context
from ..schema_check import RFC3339_STRING, schema_dtypes
from ..thread import run_in_thread_pool
from .transform import read_transform

logger = logging.getLogger("kart_import")

LIFECYCLE_COLUMNS = ("created_at", "updated_at")
"""Pipeline-managed timestamps. Normalised here rather than left to the schema so a theme with
no schema still emits the same RFC 3339 text as one with a schema."""

# Building the packed R-tree on write got too slow for the larger themes; kart reads sequentially
# and never uses it, so there's nothing it buys this pipeline.
THEME_LAYER_OPTIONS = {"SPATIAL_INDEX": "NO"} if THEME_DRIVER == "FlatGeobuf" else {}


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
    # pandas refuses a lossy float64 -> Int64 cast but silently truncates a *nullable* Float64
    # one, and `unify_dtypes` has already widened floats to Float64 by the time we get here,
    # so the check pandas would have made has to be made here instead.
    if pd.api.types.is_integer_dtype(dtype) and pd.api.types.is_float_dtype(series.dtype):
        lossy = series.notna() & (series != series.round())
        if lossy.any():
            raise ValueError(f"would truncate non-integer values {series[lossy].unique()[:5].tolist()}")
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
            else:
                merged[col] = _cast(series, target)
        except (TypeError, ValueError) as e:
            raise ValueError(f"{theme_name}.{col}: cannot cast {series.dtype} to {target}: {e}") from e

    if untyped := untyped_columns(merged):
        logger.warning(
            f"{theme_name}: no dtype for {', '.join(untyped)}; will be written as text",
            extra={"theme": theme_name, "columns": untyped},
        )

    return merged


def untyped_columns(merged: gpd.GeoDataFrame) -> list[str]:
    """The columns still on `object`, which pyogrio writes as text whatever they hold.

    Non-empty when no dataset in the release carried a value for the column (``col: null``
    everywhere, a `since_release` not yet met, a lookup predating its own history) or when the
    sources genuinely disagree, and the schema had no dtype to settle it with. The release still
    builds, but the column lands in kart as text, and a later release where a value does appear
    would type it properly.
    """
    return [
        col
        for col in merged.columns
        if col != merged.geometry.name and merged[col].dtype == object  # noqa: E721
    ]


def _theme_hash_file(theme_name: str, release_id: int) -> Path:
    """Sidecar path holding the SHA-256 of a release's merged theme file."""
    return WORKING_THEME_DIR / f"release_{release_id}" / f"{theme_name}{THEME_SUFFIX}.sha256"


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_theme_hash(theme_name: str, release_id: int, output_file: Path) -> None:
    """Hash `output_file` right after writing it and persist the digest alongside it.

    Deliberately just the hash, not a comparison against the previous release: `merge_theme_all_
    releases` runs every release's merge concurrently, so there's no guarantee the previous
    release's digest even exists yet. The comparison is `dedupe_theme_releases`'s job, run
    sequentially once every release has finished.
    """
    _theme_hash_file(theme_name, release_id).write_text(_hash_file(output_file))


def _symlink_theme_file(theme_name: str, release_id: int, source_id: int) -> None:
    """Point release `release_id`'s theme file, and its hash sidecar, at release `source_id`'s.

    The hash sidecar is a real copy (not symlinked): it's a few bytes of text, and keeping it a
    plain file means reading any release's digest never has to chase a chain of symlinks back to
    wherever the data actually lives.
    """
    output_file = WORKING_THEME_DIR / f"release_{release_id}" / f"{theme_name}{THEME_SUFFIX}"
    source_file = WORKING_THEME_DIR / f"release_{source_id}" / f"{theme_name}{THEME_SUFFIX}"

    output_file.parent.mkdir(parents=True, exist_ok=True)
    # `is_symlink` as well as `exists`: a stale symlink left dangling by a deleted target reads as
    # non-existent, and a later `symlink` call would then fail with EEXIST.
    if output_file.exists() or output_file.is_symlink():
        output_file.unlink()
    os.symlink(os.path.relpath(source_file, output_file.parent), output_file)

    _theme_hash_file(theme_name, release_id).write_text(_theme_hash_file(theme_name, source_id).read_text())


def dedupe_theme_releases(theme_name: str) -> None:
    """Symlink each release's theme file to the previous release's when their hashes match.

    Walked oldest-first over every configured release, after `merge_theme_all_releases`'s thread
    pool has fully joined, so every digest `_write_theme_hash` wrote is already on disk -- the
    ordering this needs doesn't have to fight the parallel merge for it.

    This catches output-identical releases `_theme_input_signature` didn't (different transform
    bytes that still merge to the same result); releases it already caught just redo an
    already-correct symlink here, which is harmless.
    """
    previous_id: int | None = None
    previous_hash: str | None = None

    for release in get_releases():
        hash_file = _theme_hash_file(theme_name, release.id)
        if not hash_file.exists():
            previous_id = previous_hash = None
            continue

        digest = hash_file.read_text()
        if digest == previous_hash and previous_id is not None:
            _symlink_theme_file(theme_name, release.id, previous_id)
            logger.info(
                f"{theme_name}: release {release.id} unchanged from release {previous_id}, deduplicated",
                extra={"theme": theme_name, "release": release.id, "previous_release": previous_id},
            )

        previous_id, previous_hash = release.id, digest


def _theme_input_signature(theme: Theme, release_id: int) -> str | None:
    """Combined hash of every dataset's transform file feeding this release's merge.

    None when a dataset's transform output is missing, so the caller falls through to a real
    merge attempt (and its own, better-targeted missing-input error) instead of silently
    skipping. Two releases with the same signature are guaranteed to merge to the same output:
    the merge is a pure function of these files plus the theme config/schema, neither of which
    varies by release.
    """
    parts = []
    for dataset in theme.datasets:
        transform_path = WORKING_TRANSFORM_DIR / f"release_{release_id}" / f"{dataset.name}{TRANSFORM_SUFFIX}"
        if not transform_path.exists():
            return None
        parts.append(f"{dataset.name}:{_hash_file(transform_path)}")
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


def merge_theme_release(theme_name: str, release_id: int):
    theme = get_theme_by_name(theme_name)

    logger.info(f"Merging theme: {theme.name} for release {release_id}")

    release_dir = WORKING_THEME_DIR / f"release_{release_id}"
    release_dir.mkdir(parents=True, exist_ok=True)
    output_file = release_dir / f"{theme.name}{THEME_SUFFIX}"
    gdfs = []

    # `is_symlink` as well as `exists`: a stale dedup symlink (see `dedupe_theme_releases`) left
    # dangling by a deleted target reads as non-existent, and a later `symlink` call would then
    # fail with EEXIST.
    if output_file.exists() or output_file.is_symlink():
        output_file.unlink()

    missing = []
    empty_gdfs: list[gpd.GeoDataFrame] = []
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
            empty_gdfs.append(gdf)
            continue

        logger.info(f"{dataset.name} (release {release_id}): {len(gdf)} features")
        gdfs.append(gdf)

    if missing:
        raise FileNotFoundError(
            f"{theme.name} release {release_id}: no transform output for {', '.join(missing)}. Run transform first."
        )

    if not gdfs:
        # Every dataset in the theme was empty for this release. Written rather than skipped so the
        # Snakefile gets the output it declares; `kart_import_theme` skips a featureless file so no
        # empty commit reaches kart. Merged from the empty frames, not fabricated, so the file
        # still carries the theme's columns - a geometry-only file has no `id` to key an import on.
        logger.warning(f"No data found for theme {theme.name} release {release_id}, writing empty output.")
        if not empty_gdfs:  # a theme with no datasets at all - no shape to take
            gpd.GeoDataFrame(geometry=[], crs=theme.target_epsg).to_file(
                output_file, driver=THEME_DRIVER, index=False, layer_options=THEME_LAYER_OPTIONS
            )
            _write_theme_hash(theme.name, release_id, output_file)
            return
        gdfs = empty_gdfs

    unify_dtypes(gdfs)
    merged = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)
    merged = coerce_dtypes(merged, theme.name)

    # A null/empty geometry (a source data-quality issue) has nothing to render or index anyway,
    # so drop it -- matching how the spatial lookup select drops geometryless rows.
    usable = merged.geometry.notna() & ~merged.geometry.is_empty
    dropped = int((~usable).sum())
    if dropped:
        logger.warning(
            f"{theme.name} release {release_id}: dropping {dropped} feature(s) with null/empty geometry",
            extra={"theme": theme.name, "release": release_id, "dropped": dropped},
        )
        merged = merged[usable].reset_index(drop=True)

    # Stable sorting to keep row order predictable
    if "id" in merged.columns:
        merged = merged.sort_values(by=["id"]).reset_index(drop=True)

    logger.info(f"Writing {len(merged)} total features into {output_file}")

    # Explicitly remove fid if it exists and ensure index is not written
    if "fid" in merged.columns:
        merged = merged.drop(columns=["fid"])

    merged.to_file(output_file, driver=THEME_DRIVER, index=False, layer_options=THEME_LAYER_OPTIONS)
    _write_theme_hash(theme.name, release_id, output_file)


def merge_theme_all_releases(theme_name: str, thread_count: int = 4) -> None:
    """Merge every release of one theme inside a thread pool, then dedupe across them.

    One process (and one OTel trace) for the whole theme instead of Snakemake spawning one
    process per release: at today's release count that's dozens of python startups and
    `load_from_yaml()` config reloads this avoids paying more than once.

    A release whose transform inputs are identical to an earlier one's skips the merge
    entirely (symlinked straight to that release's output) instead of redoing the read/concat/
    coerce work just to discover, via `dedupe_theme_releases`, that the result matches too.
    """
    theme = get_theme_by_name(theme_name)
    releases = get_releases()

    # Hashing (I/O-bound, one release independent of another) is worth spreading across the pool
    # too; only the sequential pass below -- comparing a release's signature to the one before it
    # -- actually needs releases in order.
    signatures = run_in_thread_pool(
        func=lambda release: _theme_input_signature(theme, release.id),
        items=releases,
        thread_count=thread_count,
        trace_items=False,
    )

    to_merge: list[Release] = []
    symlink_from: dict[int, int] = {}
    anchor: tuple[int, str] | None = None
    for release, signature in zip(releases, signatures, strict=True):
        if signature is not None and anchor is not None and signature == anchor[1]:
            symlink_from[release.id] = anchor[0]
        else:
            to_merge.append(release)
            anchor = (release.id, signature) if signature is not None else None

    def merge_release(release: Release) -> None:
        # Named (not a lambda) and wrapped in its own span so each release still traces as its own
        # "theme: <theme> [rN]", matching the span a dedicated `uv run` process used to open.
        with log_context(action="theme", theme=theme_name, release=release.id):
            merge_theme_release(theme_name, release.id)

    run_in_thread_pool(func=merge_release, items=to_merge, thread_count=thread_count, trace_items=False)

    for release_id, source_id in symlink_from.items():
        _symlink_theme_file(theme_name, release_id, source_id)
        logger.info(
            f"{theme_name}: release {release_id} has the same inputs as release {source_id}, skipped merging",
            extra={"theme": theme_name, "release": release_id, "source_release": source_id},
        )

    dedupe_theme_releases(theme_name)


if __name__ == "__main__":
    import sys

    if len(sys.argv) == 3:
        with log_context(action="theme", theme=sys.argv[1], release=int(sys.argv[2])):
            merge_theme_release(sys.argv[1], int(sys.argv[2]))
    elif len(sys.argv) == 2:
        with log_context(action="theme", theme=sys.argv[1]):
            merge_theme_all_releases(sys.argv[1])
    else:
        print("Usage: python -m kart_import.assets.theme <theme_name> [release_id]")
        sys.exit(1)
