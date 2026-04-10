import geopandas as gpd  # type: ignore

# offshore (1) or inland island (0) - intersect using sea_coastline poly shapefile
# create from coastline and outer box

islands = r"C:\Data\Topo50\Release64_NZ50_Shape\island_poly.shp"
islands_out = r"C:\Data\Topo50\Release64_NZ50_Shape\island_poly.shp"
sea_poly = r"C:\Data\Topo50\sea_polygon\sea_coastline.shp"

island_gdf = gpd.read_file(islands)

island_gdf["location"] = 0

sea_gdf = gpd.read_file(sea_poly)
crs = island_gdf.crs
sea_gdf = sea_gdf.to_crs(crs)

island_Joined_gdf = gpd.sjoin(island_gdf, sea_gdf, how="inner", predicate="intersects")
island_Joined_gdf["location"] = 1

island_gdf.loc[island_gdf.index.isin(island_Joined_gdf.index), "location"] = 1

island_gdf.to_file(islands_out, driver="ESRI Shapefile")

print("Finished processing islands.")

# POSSIBLE TEMP - update road t50_fid so not 0 - current run as SQL in postgres
# WITH numbered_rows AS (
#     SELECT ctid,
#            ROW_NUMBER() OVER () + (SELECT COALESCE(MAX(t50_fid), 0) FROM release62.road_line WHERE t50_fid > 0) AS new_fid
#     FROM release62.road_line
#     WHERE t50_fid = 0
# )
# UPDATE release62.road_line
# SET t50_fid = numbered_rows.new_fid
# FROM numbered_rows
# WHERE release62.road_line.ctid = numbered_rows.ctid;
#
# commit;
