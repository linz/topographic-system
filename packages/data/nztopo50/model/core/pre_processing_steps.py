import geopandas as gpd  # type: ignore

# offshore (1) or inland island (0) - intersect using sea_coastline poly shapefile 
# create from coastline and outer box

islands = r"C:\Data\Topo50\Release64_NZ50_Shape\island_poly.shp"
# islands = r"C:\temp\islands\island_poly.shp"
islands_out = r"C:\temp\islands\island_poly.shp"
islands_out = r"C:\Data\Topo50\Release64_NZ50_Shape\island_poly.shp"
sea_poly = r"C:\Data\Topo50\sea_polygon\sea_coastline.shp"

island_gdf = gpd.read_file(islands)

island_gdf["location"] = 0

sea_gdf = gpd.read_file(sea_poly)
sea_gdf = sea_gdf.to_crs(island_gdf.crs)

island_Joined_gdf = gpd.sjoin(island_gdf, sea_gdf, how="inner", predicate="intersects")
island_Joined_gdf["location"] = 1

island_gdf.loc[island_gdf.index.isin(island_Joined_gdf.index), "location"] = 1

island_gdf.to_file(islands_out, driver="ESRI Shapefile")

