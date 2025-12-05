import geopandas as gpd

# Path to your GeoPackage file
gpkg_path = r"c:\temp\topology_checks.gpkg"
layer_name = "toposource-building_errors_areas"
layer_name = "toposource-vegetation_errors_areas"

# Open the layer
gdf = gpd.read_file(gpkg_path, layer=layer_name)

# Validate geometries
gdf["is_valid"] = gdf.geometry.is_valid

# Print invalid geometries
print("total layer count: ", gdf.shape[0])
invalid = gdf[~gdf["is_valid"]]
print(f"Found {len(invalid)} invalid geometries.")
print(invalid)
