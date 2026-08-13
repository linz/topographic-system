import os
import geopandas as gpd  # type: ignore
import pyogrio

# Read the layer from the GeoPackage file
gpkg_path = r"C:\Data\toposource\topographic-data\topographic-data.gpkg"
gpkg_path = r"C:\Data\toposource\topographic-data-amcmenamin\topographic-data-amcmenamin.gpkg"
#gpkg_path = r"C:\Data\topoedit\topographic-data\topographic-data.gpkg"
output_path = r"C:\Data\temp"
output_path = r"C:\Data\temp\amcmenamin"
layers = []
#layers = ["landuse"]


if not layers:
    layers = [layer_name for layer_name, _geometry_type in pyogrio.list_layers(gpkg_path)]



for layer in layers:
    print(f"Processing layer: {layer}")
    gdf = gpd.read_file(gpkg_path, layer=layer)
    export_path = os.path.join(output_path, f"{layer}.parquet")
    gdf.to_parquet(
        export_path,
        engine="pyarrow",
        compression="zstd",  # type: ignore[arg-type]
        write_covering_bbox=True,
        row_group_size=50000,
    )
    print(f"Exported {layer} to {export_path}")
