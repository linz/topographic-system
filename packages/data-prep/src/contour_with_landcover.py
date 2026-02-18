import sys
import geopandas as gpd

def run(contour_path: str, landcover_path: str, overlap_path: str) -> None:
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

    overlap_gdf["landcover_feature_type"] = (
        overlap_gdf["landcover_feature_type"].fillna("other")
    )

    overlap_gdf.to_parquet(overlap_path)


if __name__ == "__main__":
    run(sys.argv[1], sys.argv[2], sys.argv[3])