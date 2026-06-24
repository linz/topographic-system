"""
Extract rock boundary lines that don't coincide coastlines, island shorelines and lake shorelines.

Produces a cartographic layer for symbolising rock outlines without duplicating
linework already drawn by the coastline, island and lake.
"""

import argparse
import logging
import math
from pathlib import Path

import geopandas as gpd
import pandas as pd
from pyproj import CRS

from data_prep.parquet_utils import write_parquet

logger = logging.getLogger(__name__)

# buffer applied to mask lines before difference
LINE_TOL_M = 1.0
# buffer converted to degrees as using geographic CRS(NZGD2000 4167)
LINE_TOL_DEG = LINE_TOL_M / ((2 * math.pi * CRS.from_epsg(4167).ellipsoid.semi_major_metre) / 360)


def run(marine_path: Path, coastline_path: Path, island_path: Path, water_path: Path, output_path: Path) -> None:
    rock_gdf = gpd.read_parquet(marine_path, filters=[("feature_type", "==", "rock")])
    rock_line_gdf = rock_gdf.assign(geometry=rock_gdf.geometry.boundary)

    coastline_gdf = gpd.read_parquet(coastline_path)

    island_gdf = gpd.read_parquet(island_path)
    island_line_gdf = island_gdf.assign(geometry=island_gdf.geometry.boundary)

    lake_gdf = gpd.read_parquet(water_path, filters=[("feature_type", "==", "lake")])
    lake_line_gdf = lake_gdf.assign(geometry=lake_gdf.geometry.boundary)

    mask_gdf = gpd.GeoDataFrame(
        geometry=pd.concat(
            [coastline_gdf.geometry, island_line_gdf.geometry, lake_line_gdf.geometry], ignore_index=True
        )
    )
    mask_buffer_gdf = gpd.GeoDataFrame(geometry=mask_gdf.geometry.buffer(LINE_TOL_DEG))

    rock_line_clip_gdf = gpd.overlay(rock_line_gdf, mask_buffer_gdf, how="difference")
    rock_line_clip_gdf = rock_line_clip_gdf

    write_parquet(rock_line_clip_gdf, output_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(
        description="Create rock line that don't coincide with coastline, island and lakes"
    )
    parser.add_argument("--marine", required=True, help="Path to marine parquet(for rock features)")
    parser.add_argument("--coastline", required=True, help="Path to coastline parquet")
    parser.add_argument("--island", required=True, help="Path to island parquet")
    parser.add_argument("--water", required=True, help="Path to water parquet(for lake features)")
    parser.add_argument("--output", required=True, help="Path to output parquet")
    args = parser.parse_args()

    run(Path(args.marine), Path(args.coastline), Path(args.island), Path(args.water), Path(args.output))
