import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyogrio

db_path = Path(r"C:\Data\Topo50\kart-source\nz_addresses\nz-roads-pilot\nz-roads-pilot.gpkg")
address_roads_layer = "nz_roads_pilot"

parquet_path = Path(r"C:\Data\temp\topographic-data-dev")
road_line_layer = "road_line.parquet"

export_folder = Path(r"C:\Temp\export\road_line_compare")

names_mismatch_layer = export_folder / "names_mismatch.parquet"
no_roadline_layer = export_folder / "no_roadline.parquet"

TARGET_CRS = "EPSG:2193"
ADDRESS_ROAD_BUFFER_METRES = 5


def load_address_roads():
    address_roads = pyogrio.read_dataframe(
        db_path,
        layer=address_roads_layer,
        columns=["full_road_name"],
        read_geometry=True,
        fid_as_index=True,
    )
    address_roads.index.name = "road_id"
    address_roads = address_roads.reset_index()
    if address_roads.geometry.name == "shape":
        address_roads = address_roads.rename_geometry("geometry")
    return address_roads.to_crs(TARGET_CRS)


def metadata_road_id(metadata):
    if not isinstance(metadata, str) or not metadata.strip():
        return None
    try:
        metadata_items = json.loads(metadata)
    except (json.JSONDecodeError, TypeError):
        return None
    if isinstance(metadata_items, dict):
        metadata_items = [metadata_items]
    if not isinstance(metadata_items, list):
        return None
    for item in metadata_items:
        if isinstance(item, dict) and item.get("source_key_name") == "road_id":
            return item.get("source_key_value")
    return None


def load_road_lines():
    road_lines = gpd.read_parquet(
        parquet_path / road_line_layer,
        columns=["id", "t50_fid", "name", "metadata", "geometry"],
    )
    road_lines["road_id"] = pd.to_numeric(
        road_lines["metadata"].map(metadata_road_id),
        errors="coerce",
    ).astype("Int64")
    return road_lines.to_crs(TARGET_CRS)


def export_name_mismatches(address_roads, road_lines):
    matched_roads = address_roads.merge(
        road_lines[["road_id", "id", "t50_fid", "name"]],
        on="road_id",
        how="inner",
        validate="one_to_many",
    )
    address_names = matched_roads["full_road_name"].astype("string").str.lower()
    road_line_names = matched_roads["name"].astype("string").str.lower()
    names_match = address_names.eq(road_line_names).fillna(
        address_names.isna() & road_line_names.isna()
    )
    name_mismatches = matched_roads.loc[~names_match].copy()
    name_mismatches.to_parquet(names_mismatch_layer, index=False)
    return name_mismatches


def export_address_roads_without_road_lines(address_roads, road_lines):
    address_road_buffers = address_roads.copy()
    halfway_points = address_road_buffers.geometry.interpolate(0.5, normalized=True)
    address_road_buffers.geometry = halfway_points.buffer(ADDRESS_ROAD_BUFFER_METRES)
    intersections = gpd.sjoin(
        address_road_buffers,
        road_lines[["geometry"]],
        how="left",
        predicate="intersects",
    )
    no_road_lines = intersections.loc[intersections["index_right"].isna()].drop(
        columns="index_right"
    )
    no_road_lines.to_parquet(no_roadline_layer, index=False)
    return no_road_lines


def main():
    export_folder.mkdir(parents=True, exist_ok=True)
    address_roads = load_address_roads()
    road_lines = load_road_lines()
    name_mismatches = export_name_mismatches(address_roads, road_lines)
    no_road_lines = export_address_roads_without_road_lines(
        address_roads, road_lines
    )
    print(f"Loaded {len(address_roads)} address roads")
    print(f"Loaded {len(road_lines)} road lines")
    print(f"Road lines with road_id: {road_lines['road_id'].notna().sum()}")
    print(f"Exported {len(name_mismatches)} name mismatches to {names_mismatch_layer}")
    print(
        f"Exported {len(no_road_lines)} address roads without road lines "
        f"to {no_roadline_layer}"
    )


if __name__ == "__main__":
    main()


