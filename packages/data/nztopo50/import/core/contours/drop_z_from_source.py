import geopandas as gpd # type: ignore
from shapely import force_2d  # type: ignore

source_contours = r"C:\Data\Topo50\lds-nz-contours-topo-150k-GPKG\nz-contours-topo-150k.gpkg"
source_layer = "nz_contours_topo_150k"
target_contours = r"C:\Data\Topo50\lds-nz-contours-topo-150k-GPKG\nz-contours-topo-150k_2d.gpkg"
target_layer = "nz_contours_topo_150k"

# Read (pyogrio/fastparquet are faster if available)
print(f"Reading {source_contours} (layer: {source_layer})...")
gdf = gpd.read_file(source_contours, layer=source_layer)

# Drop Zs (keeps geometry type, e.g., LineString→LineString)
print("Dropping Z values from geometries...")
gdf = gdf.set_geometry(gdf.geometry.apply(force_2d))

# Write back (e.g., to GPKG or Shapefile)
print(f"Writing {target_contours} (layer: {target_layer})...")
gdf.to_file(target_contours, layer=target_layer, driver="GPKG")
