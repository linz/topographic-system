import os
from time import time
import geopandas as gpd
import glob
import json

shapefiles_folder = r"c:\Data\Topo50\Release62_NZ50_Shape"
search_path = os.path.join(shapefiles_folder, "*.shp")
id_fieldname = "t50_fid"
output_file = r"c:\temp\ufid_store.json"

max_ids = {}
start_time = time()
for file in glob.glob(search_path):
    try:
        print(f"Processing {file}")
        gdf = gpd.read_file(file)
        if id_fieldname in gdf.columns:
            max_id = gdf[id_fieldname].max()
            filename = os.path.basename(file)
            max_ids[filename] = max_id
    except Exception as e:
        print(f"Error processing {file}: {e}")
        continue

# Find the highest value across all files
max_value = max(max_ids.values()) if max_ids else 0
max_ids['max_value'] = max_value

# Write to disk
with open(output_file, 'w') as f:
    json.dump(max_ids, f, indent=2)

end_time = time()
print(f"Elapsed time: {end_time - start_time} seconds")