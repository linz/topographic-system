"""Utilities for reading and writing GeoParquet files with standard settings."""

from pathlib import Path

import geopandas as gpd

# New Zealand coordinate reference systems used across the data-prep scripts.
NZGD2000 = 4167  # geographic (lon/lat) CRS used for stored/interchange data
NZTM2000 = 2193  # projected (metre) CRS used for metric operations
WEB_MERCATOR = 3857  # Web Mercator CRS underlying the WebMercatorQuad quadkey grid


def read_and_project(path: Path, target_crs: int | None = NZTM2000, **read_kwargs) -> gpd.GeoDataFrame:
    """Read a GeoParquet file, assert it is NZGD2000, and reproject to ``target_crs``.

    Inputs must be stored in NZGD2000 (EPSG:4167). Pass ``target_crs=None`` to
    keep the data in NZGD2000; otherwise it is reprojected (default NZTM2000).
    """
    gdf = gpd.read_parquet(path, **read_kwargs)
    epsg = gdf.crs.to_epsg() if gdf.crs else None
    if epsg != NZGD2000:
        raise ValueError(f"{path} must be NZGD2000 (EPSG:{NZGD2000}), got EPSG:{epsg}")
    return gdf if target_crs is None else gdf.to_crs(target_crs)


def write_parquet(gdf: gpd.GeoDataFrame, output: Path, row_group_size=2**15):
    compression_level = 17

    gdf.to_parquet(
        output,
        engine="pyarrow",
        compression="zstd",
        compression_level=compression_level,
        row_group_size=row_group_size,
        write_covering_bbox=True,
        index=False,
        schema_version="1.1.0",
    )
