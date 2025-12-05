import os
import pandas as pd

folder = r"C:\data\topo50-data\themes"
file = "nztopo_related_objects.xlsx"
source_file = os.path.join(folder, file)

source = pd.read_excel(source_file, sheet_name="Objects")

layers_info = []
for row in source.itertuples():
    object_name = row[1]
    theme = row[2]
    layer_name = row[3]
    layers = row[4].split(" ")
    for layer in layers:
        shp_name = layer
        if "_" in layer:
            feature_type = layer.split("_")[0]
            type = layer.split("_")[1]
            if layer.count("_") > 1:
                feature_type = feature_type + "_" + type
                type = layer.split("_")[2]
        else:
            # handle coastline and contours
            feature_type = layer
            type = "unknown"
            theme = layer
        layer_list = [object_name, shp_name, theme, feature_type, layer_name, type]
        layers_info.append(layer_list)

output_file = os.path.join(folder, "layers_info.xlsx")
df_layers = pd.DataFrame(layers_info, columns=["object_name", "shp_name", "theme", "feature_type", "layer_name", "type"])
df_layers.to_excel(output_file, index=False)

print(f"Layers information saved to {output_file}")