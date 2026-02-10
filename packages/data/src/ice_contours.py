import sys
import geopandas as gpd

contour_path = sys.argv[1]
landcover_path = sys.argv[2]
overlap_path = sys.argv[3]

contours_gdf = gpd.read_parquet(contour_path)
landcover_gdf = gpd.read_parquet(landcover_path)

overlap_gdf = gpd.overlay(contours_gdf, landcover_gdf, how='union', keep_geom_type=True)

overlap_gdf = overlap_gdf.rename(columns={'feature_type_1': 'feature_type'})
overlap_gdf = overlap_gdf.rename(columns={'topo_id_1': 'topo_id'})
overlap_gdf = overlap_gdf.rename(columns={'feature_type_2': 'landcover_feature_type'})
overlap_gdf = overlap_gdf.rename(columns={'topo_id_2': 'landcover_topo_id'})
overlap_gdf['landcover_feature_type'] = overlap_gdf['landcover_feature_type'].fillna('other')

overlap_gdf.to_parquet(overlap_path)