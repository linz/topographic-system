import os
import json
import subprocess
from validation.concepts.validate_dataset_alternatives import TopologyValidator

# Parameters
#kart diff origin/double-polygon-test --delta-filter=-,+,++ --output-format geojson --output diff
working_folder = r"C:\Data\kart\topographic-data\topographic-data-amcmenamin"  # Set to your kart repo path
diff_file = os.path.join(working_folder, "diff")
layer_name = "building"
schema = "topoedit"  # Adjust if needed
kart_command = [
    "kart", "diff", "origin/double-polygon-test",
    "--delta-filter=-,+,++",
    "--output-format", "geojson",
    "--output", "diff"
]
diff_file = os.path.join(diff_file, f"{layer_name}.geojson")

# Change to kart working folder
os.chdir(working_folder)

# Run kart diff command
subprocess.run(kart_command, check=True)

# Read the resulting GeoJSON diff file
with open(diff_file, "r", encoding="utf-8") as f:
    diff_data = json.load(f)

# Get layers and ids, make dictionary {layer: [ids]}
layers_dict = {}
id_field = "auto_pk"  # Adjust if your GeoJSON uses a different field for IDs
for feature in diff_data.get("features", []):
    layer = feature.get("properties", {}).get("layer", layer_name)
    fid = feature.get("properties", {}).get(id_field)
    if layer and fid is not None:
        layers_dict.setdefault(layer, []).append(fid)

print("Layers and IDs:", layers_dict)

# Build layers list for validator
layers = []
for lyr, ids in layers_dict.items():
    where_condition = f"{id_field} in ({','.join(map(str, ids))})"
    lyrname = lyr.replace('.', '_')  # Replace dots with underscores for layer name
    layers.append({
        "table": f"{schema}.{lyr}",
        "layername": f"{schema}_{lyrname}",
        "where_condition": where_condition,
    })

print("Layers for validator:", layers)
# Example: call validate_geometry_overlaps for each layer and id list
# from topology.validate_geometry_overlaps import TopologyValidator
db_url = "postgresql://postgres:landinformation@localhost:5432/topo"
output_dir = r"c:\temp"
area_crs = 2193
for layer in layers:
    where_condition = layer["where_condition"]
    validator = TopologyValidator(
        db_url=db_url,
        table=layer["table"],
        export_layername=layer["layername"],
        where_condition=where_condition,
        output_dir=output_dir,
        area_crs=area_crs
    )
    validator.run()

