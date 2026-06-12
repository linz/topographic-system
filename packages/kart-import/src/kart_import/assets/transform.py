import json
import logging
import os
import time
from pathlib import Path

import dask_geopandas as dgpd  # type: ignore[import-untyped]
import geopandas as gpd

from ..config import (
    WORKING_EXPORTS_DIR,
    WORKING_TRANSFORM_DIR,
    Release,
    Theme,
    ThemeDataset,
    get_releases,
    get_themes,
)
from ..log import log_context
from .fid_lifecycle import get_fid_lifecycle_file

logger = logging.getLogger("kart_import")


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

    for target_field, source_val in td.mapping.items():
        if not source_val:
            continue
        if source_val == "$":
            source_col = target_field
        elif source_val.startswith("$"):
            source_col = source_val[1:]
        else:
            new_data[target_field] = source_val
            continue

        if source_col in gdf.columns:
            new_data[target_field] = gdf[source_col]
        else:
            raise Exception(f"Source column not found: {source_col}")

    return gpd.GeoDataFrame(new_data, geometry=gdf.geometry, crs=gdf.crs)


def normalize_field_lifecyle(
    gdf: gpd.GeoDataFrame,
    td: ThemeDataset,
    lifecycle_data: dict,
) -> gpd.GeoDataFrame:
    # Initialize lifecycle columns
    gdf["created_at"] = None
    gdf["updated_at"] = None

    # Determine the primary key column. A configured fid_field wins so it stays
    # in sync with the lifecycle map; otherwise fall back to auto-detection.
    if td.fid_field:
        pk_col = td.fid_field
        if pk_col not in gdf.columns:
            raise Exception(f"Configured fid_field '{pk_col}' not found in {td.name} columns")
    elif "t50_fid" in gdf.columns:
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
    output_file = output_dir / f"{dataset_name}.json"

    if output_file.exists():
        logger.info("transform exists", extra={"target": output_file})
        return output_file

    output_dir.mkdir(parents=True, exist_ok=True)

    target_release_id = find_release_for_source(input_file.resolve(), dataset_name, releases)
    if target_release_id != release_id:
        logger.info("source_file transformed by another release", extra={"target_release": target_release_id})
        target_transformed_file = WORKING_TRANSFORM_DIR / f"release_{target_release_id}" / f"{dataset_name}.json"

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

    gdf.to_file(output_file, driver="GeoJSON", index=False)
    return output_file


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python -m kart_import.assets.transform <dataset_name> <release_id>")
        sys.exit(1)
    with log_context(action="transform", dataset=sys.argv[1], release=sys.argv[2]):
        transform_dataset_release(sys.argv[1], int(sys.argv[2]))
