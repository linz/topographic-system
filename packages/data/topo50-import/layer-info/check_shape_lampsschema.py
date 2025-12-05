import os
import pandas as pd

folder = r"C:\Data\Model\lamps_model"
shape_path = r"C:\Data\Topo50\shapefiles"
layer_info_path = os.path.join(folder, "schema_objects_list.txt")

# Read the text file and get the 'shp_name' column
df = pd.read_csv(layer_info_path, sep="\t")
keys = df['names']
expected_shp_files = set(f"{key}.shp" for key in keys)


# List all .shp files in the shape_path directory
actual_shp_files = set(f for f in os.listdir(shape_path) if f.lower().endswith('.shp'))


# Find differences
missing_files = expected_shp_files - actual_shp_files
extra_files = actual_shp_files - expected_shp_files

print("Missing .shp files (extra layers in file - missing shp file):")
for f in sorted(missing_files):
    print(f)

print("\nExtra .shp files (extra shapefiles):")
for f in sorted(extra_files):
    print(f)

