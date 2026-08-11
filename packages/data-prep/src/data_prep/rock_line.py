"""
Extract rock boundary lines that don't coincide coastlines, island shorelines and lake shorelines.

Produces a cartographic layer for symbolising rock outlines without duplicating
linework already drawn by the coastline, island and lake.
"""

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import geopandas as gpd
import pandas as pd

from data_prep.identity import earliest_created_at, reproducible_uuid7
from data_prep.parquet_utils import NZGD2000, read_and_project, write_parquet

logger = logging.getLogger(__name__)

OUTPUT_COLUMNS = [
    "id",
    "created_at",
    "updated_at",
    "t50_fid",
    "marine_id",
    "type",
    "name",
    "subtype",
    "metadata",
    "geometry",
]


# Mask buffer is a metric operation, so buffer in NZTM2000(projected metres) instead of NZGD2000(geographic degrees)
# Doing the buffer in degrees is anisotropic(E-W radius shrinks by cos(latitude)) giving an error of about 0.25m on a 1m buffer
# Even though Chathams/Kermadecs sit outside NZTM2000, it gives us a smaller error of about 0.009m
# As we are working out if rock line sits on a shoreline rather than a measurement, that sub-cm distortion is acceptable
LINE_TOL_M = 1.0


def run(marine_path: Path, coastline_path: Path, island_path: Path, water_path: Path, output_path: Path) -> None:
    rock_gdf = read_and_project(marine_path, filters=[("type", "==", "rock")])
    rock_line_gdf = rock_gdf.assign(geometry=rock_gdf.geometry.boundary)
    rock_line_gdf = rock_line_gdf.rename(columns={"id": "marine_id"})
    rock_line_gdf = rock_line_gdf.assign(t50_fid=None)

    coastline_gdf = read_and_project(coastline_path)

    island_gdf = read_and_project(island_path)
    island_line_gdf = island_gdf.assign(geometry=island_gdf.geometry.boundary)

    lake_gdf = read_and_project(water_path, filters=[("type", "==", "lake")])
    lake_line_gdf = lake_gdf.assign(geometry=lake_gdf.geometry.boundary)

    mask_gdf = gpd.GeoDataFrame(
        geometry=pd.concat(
            [coastline_gdf.geometry, island_line_gdf.geometry, lake_line_gdf.geometry], ignore_index=True
        )
    )
    mask_buffer_gdf = gpd.GeoDataFrame(geometry=mask_gdf.geometry.buffer(LINE_TOL_M))

    rock_line_clip_gdf = gpd.overlay(rock_line_gdf, mask_buffer_gdf, how="difference")
    rock_line_clip_gdf = rock_line_clip_gdf.to_crs(NZGD2000)

    # Derive a reproducible UUIDv7 from the source timestamp and the geometry.
    source_created_at = earliest_created_at(rock_gdf)
    produced_at = datetime.now(ZoneInfo("Pacific/Auckland"))
    timestamp_ms = int(pd.Timestamp(source_created_at).timestamp() * 1000)
    rock_line_clip_gdf["id"] = [
        str(reproducible_uuid7(timestamp_ms, f"rock_line/{geom.wkb_hex}")) for geom in rock_line_clip_gdf.geometry
    ]

    # Ensure required schema properties exist with expected shapes and types.
    rock_line_clip_gdf["created_at"] = source_created_at.isoformat()
    rock_line_clip_gdf["updated_at"] = produced_at.isoformat()
    rock_line_clip_gdf["t50_fid"] = pd.to_numeric(rock_line_clip_gdf["t50_fid"], errors="coerce").astype("Int64")
    rock_line_clip_gdf = rock_line_clip_gdf.reindex(columns=OUTPUT_COLUMNS)

    write_parquet(rock_line_clip_gdf, output_path)


@dataclass
class RockLineArgs:
    marine_path: Path
    coastline_path: Path
    island_path: Path
    water_path: Path
    output_path: Path


def parse_args() -> RockLineArgs:
    parser = argparse.ArgumentParser(
        description="Create rock line that don't coincide with coastline, island and lakes"
    )
    parser.add_argument(
        "--marine", type=str, dest="marine_path", required=True, help="Path to marine parquet(for rock features)"
    )
    parser.add_argument("--coastline", type=str, dest="coastline_path", required=True, help="Path to coastline parquet")
    parser.add_argument("--island", type=str, dest="island_path", required=True, help="Path to island parquet")
    parser.add_argument(
        "--water", type=str, dest="water_path", required=True, help="Path to water parquet(for lake features)"
    )
    parser.add_argument("--output", type=str, dest="output_path", required=True, help="Path to output parquet")

    parsed = parser.parse_args()

    # Validate input parquet files
    for label, path in (
        ("marine", parsed.marine_path),
        ("coastline", parsed.coastline_path),
        ("island", parsed.island_path),
        ("water", parsed.water_path),
    ):
        if not os.path.isfile(path):
            sys.stderr.write(f"Error: {label} parquet file does not exist: {path}\n")
            sys.exit(1)

    # Ensure output directory exists
    output_path = Path(parsed.output_path)
    if not os.path.isdir(output_path.parent):
        try:
            os.makedirs(output_path.parent, exist_ok=True)
        except Exception as e:
            sys.stderr.write(f"Error: Output directory could not be created: {output_path.parent}. Details: {e}\n")
            sys.exit(1)

    return RockLineArgs(
        marine_path=Path(parsed.marine_path),
        coastline_path=Path(parsed.coastline_path),
        island_path=Path(parsed.island_path),
        water_path=Path(parsed.water_path),
        output_path=output_path,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    args = parse_args()

    run(args.marine_path, args.coastline_path, args.island_path, args.water_path, args.output_path)
