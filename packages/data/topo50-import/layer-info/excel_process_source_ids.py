# This scripts extracts LAMPS object names into layers names and types
import os
import pandas as pd

path = r"C:\Data\Topo50\themes"
file = os.path.join(path, "topo50-data-layers-sources-update-methods-copy.xlsx")

df = pd.read_excel(file, sheet_name="Layer List", engine='openpyxl')
#print(df.columns)
#print(df.head())

# Iterate over each row, read the first field, and add new fields
for idx, row in df.iterrows():
    lds_id = row.iloc[0]
    values = lds_id.split("-")
    lds_code = values[0] if len(values) > 0 else None
    name = ''
    for i, val in enumerate(values[2:]):
        if val == 'topo':
            break
        elif i == 0:
            name = val
        else:
            name = name + "_" + val

    if 'points' in name:
        key = name.replace('points', 'pnt')
    elif 'lines' in name:
        key = name.replace('centrelines', 'cl')
    elif 'edges' in name:
        key = name.replace('edges', 'edge')
    elif 'polygons' in name:
        key = name.replace('polygons', 'poly') 
    else:
        key = name

    #print(name, key, lds_code)
    df.at[idx, 'name'] = name
    df.at[idx, 'key'] = key
    df.at[idx, 'lds_code'] = lds_code

# Save the updated DataFrame to a new Excel file
output_file = os.path.join(path, "topo50-data-layers-sources.xlsx")  
df.to_excel(output_file, index=False, engine='openpyxl')
print(f"Updated DataFrame saved to {output_file}")

#print(df.head())