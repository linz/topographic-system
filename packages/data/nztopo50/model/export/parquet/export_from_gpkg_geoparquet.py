import os
import geopandas as gpd  # type: ignore

# Read the 'building' layer from the GeoPackage file
gpkg_path = r"C:\Data\topoedit\topographic-data\topographic-data.gpkg"
output_path = r"C:\Data\temp"
layers = ["building", "building_point"]

for layer in layers:
    print(f"Processing layer: {layer}")
    buildings_gdf = gpd.read_file(gpkg_path, layer=layer)
    output_path = os.path.join(output_path, f"{layer}.parquet")
    buildings_gdf.to_parquet(
        output_path,
        engine="pyarrow",
        compression="zstd",  # type: ignore[arg-type]
        write_covering_bbox=True,
        row_group_size=50000,
    )
    print(f"Exported {layer} to {output_path}")
