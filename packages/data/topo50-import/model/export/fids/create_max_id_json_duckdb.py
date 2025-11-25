import os
import glob
import json
import duckdb
import time

shapefiles_folder = r"c:\Data\Topo50\Release62_NZ50_Shape"
search_path = os.path.join(shapefiles_folder, "*.shp")
id_fieldname = "t50_fid"
output_file = r"c:\temp\ufid_store.json"

con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")
start_time = time.time()
max_ids = {}
for file in glob.glob(search_path):
    filename = os.path.basename(file)
    query = f"SELECT MAX({id_fieldname}) FROM st_read('{file}')"
    print("processing: ", query)
    try:
        result = con.execute(query).fetchone()
        max_id = result[0] if result else None
        if max_id is not None:
            max_ids[filename] = max_id
    except Exception as e:
        print(f"Error processing {filename}: {e}")

max_value = max(max_ids.values()) if max_ids else 0
max_ids['max_value'] = max_value

with open(output_file, 'w') as f:
    json.dump(max_ids, f, indent=2)

con.close()
end_time = time.time()
print(f"Elapsed time: {end_time - start_time} seconds")