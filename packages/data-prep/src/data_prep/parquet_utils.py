"""Utilities for writing GeoParquet files with standard settings."""

from pathlib import Path

import geopandas as gpd  # type: ignore


def write_parquet(gdf: gpd.GeoDataFrame, output: Path, row_group_size=50000):
    compression_level = 19

    gdf.to_parquet(
        output,
        engine="pyarrow",
        compression="zstd",
        compression_level=compression_level,
        row_group_size=row_group_size,
        write_covering_bbox=True,
        schema_version="1.1.0",
    )
