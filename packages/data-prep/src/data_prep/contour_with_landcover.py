import sys
import geopandas as gpd

from pathlib import Path


def run(contour_path: Path, landcover_path: Path, overlap_path: Path) -> None:
    contour_gdf = gpd.read_parquet(contour_path)
    landcover_gdf = gpd.read_parquet(landcover_path)

    overlap_gdf = gpd.overlay(
        contour_gdf,
        landcover_gdf,
        how="union",
        keep_geom_type=True,
    )

    overlap_gdf = overlap_gdf.rename(
        columns={
            "feature_type_1": "feature_type",
            "topo_id_1": "topo_id",
            "feature_type_2": "landcover_feature_type",
            "topo_id_2": "landcover_topo_id",
        }
    )

    overlap_gdf["landcover_feature_type"] = overlap_gdf[
        "landcover_feature_type"
    ].fillna("other")

    compression_level = 19
    row_group_size = 10000
    overlap_gdf.to_parquet(
        overlap_path,
        engine="pyarrow",
        compression="zstd",
        compression_level=compression_level,
        row_group_size=row_group_size,
        write_covering_bbox=True,
        schema_version="1.1.0",
    )


if __name__ == "__main__":
    run(Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]))
