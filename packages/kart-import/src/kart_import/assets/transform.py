from ..config import WORKING_LIFECYCLE_DIR
import json
import os

from dagster import AssetExecutionContext, AssetKey, AssetsDefinition, asset
from ..git.kart import is_kart

from ..command import run_command
from ..config import (
    SOURCE_DIR,
    WORKING_EXPORTS_DIR,
    WORKING_TRANSFORM_DIR,
    Release,
    Theme,
    ThemeDataset,
    get_releases,
    get_datasets,
    get_dataset_name,
    get_themes,
)
from ..git.release import get_release_commit
import geopandas as gpd


def normalize_projection(context: AssetExecutionContext, gdf: gpd.GeoDataFrame, target_epsg: str) -> gpd.GeoDataFrame:
    if gdf.crs != target_epsg:
        # Reduce precision to 1mm (3 decimal places in EPSG:2193 meters)
        context.log.info(f"Reducing precision of {dataset.name} to 1mm")
        gdf.geometry = gdf.geometry.set_precision(0.001)

        # Transform to NZGD2000 (EPSG:4167)
        context.log.info(f"Transforming {dataset.name} to {target_epsg}")
        gdf = gdf.to_crs(target_epsg)

    # Reduce precision again after transform to avoid floating point noise in degrees
    # 1e-8 degrees is approximately 1.1mm
    context.log.info(f"Reducing precision of {dataset.name} to 1e-8 degrees (~1mm)")
    gdf.geometry = gdf.geometry.set_precision(1e-8)

    return gdf


def normalize_fields(context: AssetExecutionContext, gdf: gpd.GeoDataFrame, td: ThemeDataset) -> gpd.GeoDataFrame:
    new_data = {}
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

def normalize_field_lifecyle(context: AssetExecutionContext, gdf: gpd.GeoDataFrame, td: ThemeDataset, lifecycle_data: dict) -> gpd.GeoDataFrame:
    # Initialize lifecycle columns
    gdf["created_at"] = None
    gdf["updated_at"] = None

    primary_key = gdf["t50_fid"].astype(str)
        
        # Map created_at
    gdf["created_at"] = primary_key.map(lambda x: lifecycle_data.get(x, {}).get("created_at"))
        # Look up pre-computed UUIDv8 id from lifecycle data
    def get_feature_id(fid):
        found = lifecycle_data.get(fid, {}).get("id")
        if found:
            return found
        raise Exception("primary_key not found in lifecycle?")

    gdf["id"] = primary_key.map(get_feature_id)

    return gdf

def _transform_dataset_release(context: AssetExecutionContext, theme: Theme, td: ThemeDataset, releases: list[Release]):
    dataset_name = get_dataset_name(td.source)

    lifecycle_file = WORKING_LIFECYCLE_DIR / f"{dataset.name}.json"
    lifecycle_data = {}
    if not lifecycle_file.exists():
        raise Exception(f"missing lifecycle_{dataset_name}")
    with open(lifecycle_file, 'r') as f:
        lifecycle_data = json.load(f)

    for release in releases:
        input_file = WORKING_EXPORTS_DIR / f"release_{release.id}" / f"{dataset_name}.json"
        output_dir = WORKING_TRANSFORM_DIR / f"release_{release.id}"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{dataset_name}.json"

        if output_file.exists():
            output_file.unlink()  # todo should we keep existing transforms at some point
            # context.log.info(f"Output exists {output_file}, skipping...")
            # continue

        if input_file.is_symlink():
            previous_output = WORKING_TRANSFORM_DIR / f"release_{release.id - 1}" / f"{dataset_name}.json"
            if output_file.exists() or output_file.is_symlink():
                output_file.unlink()
            os.symlink(previous_output, output_file)
            continue

        context.log.info(f"Reading {input_file}")
        gdf = gpd.read_file(input_file, engine="pyogrio")

        if gdf.crs is None:
            raise Exception("source frame has no projection")

        gdf = normalize_projection(context, gdf, "EPSG:4167")
        gdf = normalize_fields(context, gdf, td)
        gdf = normalize_field_lifecyle(context, gdf, td, lifecycle_data)

        gdf.to_file(output_file, driver="GeoJSON", index=False)


def make_dataset_export_transform_asset(theme: Theme, td: ThemeDataset, releases: list[Release]) -> AssetsDefinition:
    dataset_name = get_dataset_name(td.source)

    @asset(name=f"transform_{dataset_name}", group_name="transform", deps=[
        AssetKey(f"lifecycle_{dataset_name}"),
        AssetKey(f"release_{dataset_name}")
    ])
    def _transform_asset(context: AssetExecutionContext):
        return _transform_dataset_release(context, theme, td, releases)

    return _transform_asset


selected_releases = get_releases()
release_assets = []

for theme in get_themes():
    for dataset in theme.datasets:
        release_assets.append(make_dataset_export_transform_asset(theme, dataset, selected_releases))
