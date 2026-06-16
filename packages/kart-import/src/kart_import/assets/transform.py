import fcntl
import json
import logging
import os
import time
from contextlib import contextmanager
from pathlib import Path

import dask_geopandas as dgpd  # type: ignore[import-untyped]
import geopandas as gpd
import pandas as pd

from ..config import (
    SOURCE_DIR,
    TRANSFORM_FORMAT,
    TRANSFORM_SUFFIX,
    WORKING_EXPORTS_DIR,
    WORKING_LOOKUP_DIR,
    WORKING_TRANSFORM_DIR,
    Release,
    Theme,
    ThemeDataset,
    get_lookup_by_name,
    get_releases,
    get_themes,
)
from ..corrections import apply_corrections
from ..fixups import FIXUPS
from ..git.release import get_release_commit
from ..log import log_context
from .fid_lifecycle import get_fid_lifecycle_file

logger = logging.getLogger("kart_import")


def write_transform(gdf: gpd.GeoDataFrame, output_file: Path) -> None:
    """Write a transform intermediate in the configured format (see TRANSFORM_FORMAT)."""
    if TRANSFORM_FORMAT == "parquet":
        gdf.to_parquet(output_file, compression="zstd", index=False)
    else:
        gdf.to_file(output_file, driver="GeoJSON", index=False)


def read_transform(path: Path) -> gpd.GeoDataFrame:
    """Read a transform intermediate written by `write_transform`."""
    if TRANSFORM_FORMAT == "parquet":
        return gpd.read_parquet(path)
    return gpd.read_file(path, engine="pyogrio", use_arrow=True)


def _fixup_applies(fixup, release_id: int) -> bool:
    return fixup.releases is None or release_id in fixup.releases


def apply_fixups(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Run the dataset's configured fixups for this release, in order."""
    for fixup in td.fixups:
        if not _fixup_applies(fixup, release_id):
            continue
        logger.info("apply_fixup", extra={"fn": fixup.fn, "dataset": td.name, "release": release_id})
        gdf = FIXUPS[fixup.fn](gdf, td, release_id)
    return gdf


def _normalise_join_key(series: pd.Series) -> pd.Series:
    """Canonicalise a join key so int/float/string forms match (e.g. 5.0 == 5 == '5')."""
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().any():
        return numeric.astype("Int64").astype("string")  # FIXME: This will clobber leading-zero string keys. Remove / update if needed.
    return series.astype("string")


def _resolve_lookup_commit(lookup_name: str, release_id: int) -> str | None:
    """The lookup commit as-of this release, or None if the release predates any commits."""
    release = next((r for r in get_releases() if r.id == release_id), None)
    if release is None:
        return None
    res = get_release_commit(SOURCE_DIR / lookup_name, release.until)
    return res[0] if res is not None else None


def apply_joins(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Left-join each configured lookup's columns onto the source frame by key."""
    for join in td.joins:
        lookup = get_lookup_by_name(join.lookup)
        wanted = join.columns or list(lookup.columns)

        commit = _resolve_lookup_commit(join.lookup, release_id)
        if commit is None:
            logger.info(
                "no lookup for this release; filling join columns null",
                extra={"dataset": td.name, "lookup": join.lookup, "release": release_id, "columns": wanted},
            )
            gdf = gdf.copy()
            for col in wanted:
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
        right = right.drop(columns=[lookup.key])

        gdf = gdf.copy()
        gdf["__join_key__"] = _normalise_join_key(gdf[join.left_on])
        merged = gdf.merge(right, on="__join_key__", how="left").drop(columns="__join_key__")
        gdf = gpd.GeoDataFrame(merged, geometry=gdf.geometry.name, crs=gdf.crs)
        matched = int(gdf[wanted[0]].notna().sum()) if wanted else 0
        logger.info(
            "apply_join",
            extra={"dataset": td.name, "lookup": join.lookup, "columns": wanted, "matched_rows": matched},
        )
    return gdf


def get_theme_and_dataset(dataset_name: str) -> tuple[Theme, ThemeDataset]:
    for theme in get_themes():
        for dataset in theme.datasets:
            if dataset.name == dataset_name:
                return theme, dataset
    raise Exception(f"Theme not found for dataset: {dataset_name}")


def normalize_projection(gdf: gpd.GeoDataFrame, td: ThemeDataset, target_epsg: str) -> gpd.GeoDataFrame:
    if gdf.crs == target_epsg:
        # Reduce precision to avoid floating point noise in degrees
        # 1e-8 degrees is approximately 1.1mm
        logger.info(f"Reducing precision of {td.name} to 1e-8 degrees (~1mm)")
        gdf.geometry = gdf.geometry.set_precision(1e-8)
        return gdf

    ddf = dgpd.from_geopandas(gdf, npartitions=4)
    ddf = ddf.to_crs(target_epsg)

    def apply_precision(df):
        df.geometry = df.geometry.set_precision(1e-8)
        return df

    ddf = ddf.map_partitions(apply_precision, meta=ddf._meta)
    gdf = ddf.compute()
    return gdf


def normalize_fields(gdf: gpd.GeoDataFrame, td: ThemeDataset) -> gpd.GeoDataFrame:
    new_data = {
        "id": gdf["id"],
        "created_at": gdf["created_at"],
        "updated_at": gdf["updated_at"],
    }

    for target_field, spec in td.field_specs().items():
        source = spec.source

        # A column reference ("$" / "$col"); anything else is a literal constant.
        if isinstance(source, str) and source.startswith("$"):
            source_col = target_field if source == "$" else source[1:]
            if source_col not in gdf.columns:
                raise Exception(f"Source column not found: {source_col} in {td.name}")
            values = gdf[source_col]
            if spec.default is not None:
                values = values.fillna(spec.default)
            new_data[target_field] = values
        elif source is None:
            new_data[target_field] = spec.default
        else:
            new_data[target_field] = source

    return gpd.GeoDataFrame(new_data, geometry=gdf.geometry, crs=gdf.crs)


def normalize_field_lifecyle(
    gdf: gpd.GeoDataFrame,
    td: ThemeDataset,
    lifecycle_data: dict,
) -> gpd.GeoDataFrame:
    # Initialize lifecycle columns
    gdf["created_at"] = None
    gdf["updated_at"] = None

    # Detect the primary key column
    if "t50_fid" in gdf.columns:
        pk_col = "t50_fid"
    elif "auto_pk" in gdf.columns:
        pk_col = "auto_pk"
    else:
        raise Exception("Neither t50_fid nor auto_pk found in dataset columns")

    primary_key = gdf[pk_col].astype(str)

    gdf["created_at"] = primary_key.map(lambda x: lifecycle_data.get(x, {}).get("created_at"))
    gdf["updated_at"] = gdf["created_at"]

    # Look up pre-computed UUIDv7 id from lifecycle data
    def get_feature_id(fid):
        found = lifecycle_data.get(fid, {}).get("id")
        if found:
            return found
        raise Exception(f"primary_key: {fid} not found in lifecycle?")

    gdf["id"] = primary_key.map(get_feature_id)

    return gdf


def find_release_for_source(source_file: Path, dataset_name: str, releases: list[Release]) -> int:
    """
    Some releases will point to the same file,
    we want only want to process the file if we really have to.
    """
    for release in releases:
        input_file = WORKING_EXPORTS_DIR / f"release_{release.id}" / f"{dataset_name}.json"
        if input_file.exists() and input_file.resolve() == source_file:
            return release.id
    raise Exception(f"No release found for source file: {source_file}")


@contextmanager
def exclusive_lock(target_file: Path):
    """Hold an exclusive cross-process lock keyed on `target_file`."""
    lock_path = target_file.with_name(f"{target_file.name}.lock")
    with open(lock_path, "w") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def wait_for_file_exists(target_file: Path, timeout: int = 5):
    logger.info(
        "checking/waiting for target file",
        extra={"target_file": str(target_file)},
    )
    for _ in range(timeout):
        if target_file.exists():
            return
        time.sleep(1)


def transform_dataset_release(dataset_name: str, release_id: int, wait_for_release: bool = False) -> Path:
    theme, td = get_theme_and_dataset(dataset_name)
    releases = get_releases()

    input_file = WORKING_EXPORTS_DIR / f"release_{release_id}" / f"{dataset_name}.json"
    if not input_file.exists():
        raise Exception(f"'export' file missing: {input_file}")

    output_dir = WORKING_TRANSFORM_DIR / f"release_{release_id}"
    output_file = output_dir / f"{dataset_name}{TRANSFORM_SUFFIX}"

    if output_file.exists():
        logger.info("transform exists", extra={"target": output_file})
        return output_file

    output_dir.mkdir(parents=True, exist_ok=True)

    target_release_id = find_release_for_source(input_file.resolve(), dataset_name, releases)
    if target_release_id != release_id:
        gated_here = [f.fn for f in td.fixups if f.releases is not None and release_id in f.releases]
        if gated_here:
            raise Exception(
                f"fixup(s) {gated_here} target release {release_id}, which shares a source file with release "
                f"{target_release_id} and is not transformed on its own; gate the fixup to the canonical "
                f"release {target_release_id} instead"
            )
        logger.info("source_file transformed by another release", extra={"target_release": target_release_id})
        target_transformed_file = (
            WORKING_TRANSFORM_DIR / f"release_{target_release_id}" / f"{dataset_name}{TRANSFORM_SUFFIX}"
        )

        # Target file should be created by another process if we are running directly via __main__ create the other
        # releases file, otherwise wait for the target file to exist
        if wait_for_release:
            wait_for_file_exists(target_transformed_file)
        else:
            with log_context(
                action="transform", dataset=dataset_name, release=target_release_id, parent_release=release_id
            ):
                transform_dataset_release(dataset_name, target_release_id)

        if not target_transformed_file.exists():
            raise Exception(f"failed to wait for target: {target_transformed_file}")

        os.symlink(os.path.relpath(target_transformed_file, output_dir), output_file)
        logger.info("symlinked")
        return output_file

    with exclusive_lock(output_file):
        if output_file.exists():
            logger.info("transform produced while waiting for lock", extra={"target": output_file})
            return output_file

        lifecycle_file = get_fid_lifecycle_file(dataset_name, releases)
        if not lifecycle_file.exists():
            raise Exception(f"missing lifecycle_{dataset_name}")
        with open(lifecycle_file) as f:
            lifecycle_data = json.load(f)

        start_time = time.perf_counter()
        gdf = gpd.read_file(input_file, engine="pyogrio", use_arrow=True)
        logger.info("read_source", extra={"duration": round(time.perf_counter() - start_time, 4)})

        if gdf.crs is None:
            raise Exception("source frame has no projection")

        if td.joins:
            start_time = time.perf_counter()
            gdf = apply_joins(gdf, td, release_id)
            logger.info("apply_joins", extra={"duration": round(time.perf_counter() - start_time, 4)})

        start_time = time.perf_counter()
        gdf = normalize_field_lifecyle(
            gdf,
            td,
            lifecycle_data,
        )
        logger.info("normalize_field_lifecyle", extra={"duration": round(time.perf_counter() - start_time, 4)})

        start_time = time.perf_counter()
        gdf = normalize_projection(gdf, td, theme.target_epsg)
        logger.info("normalize_projection", extra={"duration": round(time.perf_counter() - start_time, 4)})

        start_time = time.perf_counter()
        gdf = normalize_fields(gdf, td)
        logger.info("normalize_fields", extra={"duration": round(time.perf_counter() - start_time, 4)})

        if td.corrections:
            start_time = time.perf_counter()
            gdf = apply_corrections(gdf, td)
            logger.info("apply_corrections", extra={"duration": round(time.perf_counter() - start_time, 4)})

        if td.fixups:
            start_time = time.perf_counter()
            gdf = apply_fixups(gdf, td, release_id)
            logger.info("apply_fixups", extra={"duration": round(time.perf_counter() - start_time, 4)})

        write_transform(gdf, output_file)
    return output_file


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python -m kart_import.assets.transform <dataset_name> <release_id>")
        sys.exit(1)
    with log_context(action="transform", dataset=sys.argv[1], release=sys.argv[2]):
        transform_dataset_release(sys.argv[1], int(sys.argv[2]))
