import os
from pyogrio import write_dataframe, read_dataframe, list_layers
import csv


@staticmethod
def read_dataset(extension, input_file, layer_name=None, bbox=None):
    try:
        if extension == "geojson":
            gdf = read_dataframe(input_file, driver="GeoJSON", bbox=bbox)
        elif extension == "gpkg":
            gdf = read_dataframe(input_file, layer=layer_name, driver="GPKG", bbox=bbox)
        else:
            gdf = read_dataframe(input_file, layer=layer_name, bbox=bbox)
    except Exception as e:
        print(f"Error reading layer '{layer_name}' to '{input_file}': {e}")
        return None
    return gdf


@staticmethod
def write_dataset(
    extension, gdf, output_file, layer_name=None, dataset_name=None, append_data=True
):
    try:
        if extension == "geojson":
            write_dataframe(gdf, output_file, driver="GeoJSON")
        elif extension == "gpkg":
            write_dataframe(gdf, output_file, layer=layer_name, driver="GPKG")
        else:
            write_dataframe(
                gdf,
                output_file,
                layer=layer_name,
                driver="OpenFileGDB",
                append=append_data,
                FEATURE_DATASET=dataset_name,
                TARGET_ARCGIS_VERSION="ARCGIS_PRO_3_2_OR_LATER",
            )
    except Exception as e:
        print(f"Error writing layer '{layer_name}' to '{output_file}': {e}")


data_folder = r"C:\Data\topo50Editor"
data_file = os.path.join(data_folder, "topo50data_themes.gdb")
output_file = os.path.join(data_folder, "topo50data_sample.gdb")

datasets_file = os.path.join(data_folder, "datasets.csv")

bbox = (1200000, 4975000, 1380000, 5100000)

datasets_dict = {}
with open(datasets_file, newline="", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        dataset = row.get("dataset")
        layer = row.get("layer")
        if dataset and layer:
            datasets_dict[layer] = dataset

layers = list_layers(data_file)
for layer in layers:
    layer = layer[0]
    print(f"Layer: {layer}")
    gdf = read_dataset("OpenFileGDB", input_file=data_file, layer_name=layer, bbox=bbox)

    if gdf is not None:
        write_dataset(
            "OpenFileGDB",
            gdf,
            output_file,
            layer_name=layer,
            dataset_name=datasets_dict[layer],
            append_data=True,
        )
