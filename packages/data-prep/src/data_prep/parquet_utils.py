from pathlib import Path
import geopandas as gpd


def write_parquet(gdf: gpd.GeoDataFrame, output: Path):
    compression_level = 19
    row_group_size = 10000

    gdf.to_parquet(
        output,
        engine="pyarrow",
        compression="zstd",
        compression_level=compression_level,
        row_group_size=row_group_size,
        write_covering_bbox=True,
        schema_version="1.1.0",
    )