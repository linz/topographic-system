import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
import dask_geopandas as dgpd  # type: ignore[import-untyped]

import geopandas as gpd
from dagster import AssetExecutionContext, AssetKey, AssetsDefinition, asset

from ..config import (
    WORKING_EXPORTS_DIR,
    WORKING_LIFECYCLE_DIR,
    WORKING_TRANSFORM_DIR,
    Release,
    Theme,
    ThemeDataset,
    get_dataset_name,
    get_releases,
    get_themes,
)


def normalize_projection(
    context: AssetExecutionContext, gdf: gpd.GeoDataFrame, td: ThemeDataset, target_epsg: str
) -> gpd.GeoDataFrame:
    if gdf.crs == target_epsg:
        # Reduce precision to avoid floating point noise in degrees
        # 1e-8 degrees is approximately 1.1mm
        context.log.info(f"Reducing precision of {td.name} to 1e-8 degrees (~1mm)")
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


def normalize_fields(context: AssetExecutionContext, gdf: gpd.GeoDataFrame, td: ThemeDataset) -> gpd.GeoDataFrame:
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
    context: AssetExecutionContext,
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


def _transform_dataset_release(context: AssetExecutionContext, theme: Theme, td: ThemeDataset, releases: list[Release]):
    dataset_name = td.name

    lifecycle_file = WORKING_LIFECYCLE_DIR / f"{dataset_name}.json"
    lifecycle_data = {}
    if not lifecycle_file.exists():
        raise Exception(f"missing lifecycle_{dataset_name}")
    with open(lifecycle_file) as f:
        lifecycle_data = json.load(f)

    def process_release(release: Release):
        input_file = WORKING_EXPORTS_DIR / f"release_{release.id}" / f"{dataset_name}.json"
        output_dir = WORKING_TRANSFORM_DIR / f"release_{release.id}"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{dataset_name}.json"

        if output_file.exists():
            output_file.unlink()  # todo should we keep existing transforms at some point

        if input_file.is_symlink():
            previous_output = WORKING_TRANSFORM_DIR / f"release_{release.id - 1}" / f"{dataset_name}.json"
            if output_file.exists() or output_file.is_symlink():
                output_file.unlink()
            os.symlink(previous_output, output_file)
            return

        context.log.info(f"Reading {input_file}")
        start_time = time.perf_counter()
        gdf = gpd.read_file(input_file, engine="pyogrio", use_arrow=True)

        duration = time.perf_counter() - start_time
        context.log.info(f"{release.id}: read_file took {duration:.4f}s")

        if gdf.crs is None:
            raise Exception("source frame has no projection")

        start_time = time.perf_counter()
        gdf = normalize_field_lifecyle(
            context,
            gdf,
            td,
            lifecycle_data,
        )
        lifecycle_duration = time.perf_counter() - start_time
        context.log.info(f"{release.id}: normalize_field_lifecyle took {lifecycle_duration:.4f}s")

        start_time = time.perf_counter()
        gdf = normalize_projection(context, gdf, td, theme.target_epsg)
        projection_duration = time.perf_counter() - start_time
        context.log.info(f"{release.id}: normalize_projection took {projection_duration:.4f}s")

        start_time = time.perf_counter()
        gdf = normalize_fields(context, gdf, td)
        fields_duration = time.perf_counter() - start_time
        context.log.info(f"{release.id}: normalize_fields took {fields_duration:.4f}s")

        gdf.to_file(output_file, driver="GeoJSON", index=False)

    for release in releases:
        process_release(release)


def make_dataset_export_transform_asset(theme: Theme, td: ThemeDataset, releases: list[Release]) -> AssetsDefinition:
    dataset_name = get_dataset_name(td.source)

    @asset(
        name=f"transform_{dataset_name}",
        group_name="transform",
        deps=[AssetKey(f"lifecycle_{dataset_name}"), AssetKey(f"release_{dataset_name}")],
    )
    def _transform_asset(context: AssetExecutionContext):
        return _transform_dataset_release(context, theme, td, releases)

    return _transform_asset


selected_releases = get_releases()
release_assets = []

for theme in get_themes():
    for dataset in theme.datasets:
        release_assets.append(make_dataset_export_transform_asset(theme, dataset, selected_releases))
