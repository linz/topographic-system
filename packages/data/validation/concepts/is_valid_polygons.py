
import geopandas as gpd
from shapely import coverage_is_valid
import time

start_time = time.time()
# Load geometries from a GeoPackage, Shapefile, or other source
gdf = gpd.read_file(r"C:\Data\kart\topo50data.gpkg", layer="vegetation")

# Extract the geometries
geoms = gdf.geometry.values
# Check if the coverage is valid
is_valid = coverage_is_valid(geoms)

print("Coverage is valid:", is_valid)
end_time = time.time()
print("Start time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time)))
print("End time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time)))
print("Duration (seconds):", end_time - start_time)
