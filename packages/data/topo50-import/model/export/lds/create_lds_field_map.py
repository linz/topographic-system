import os
import glob
import fiona
import pandas as pd
from osgeo import ogr
import json

def ogr_fields():
    ds = ogr.Open(filename)
    layer = ds.GetLayer()
    layer_name = layer.GetName()
    geom_type = layer.GetGeomType()
    geom_name = ogr.GeometryTypeToName(geom_type)
    if geom_type > 3:
        print(f"Unsupported geometry type {geom_name} in {filename}")
    if geom_name == 'Line String':
        geom_name = 'LineString'
 
    properties = {}
    layer_defn = layer.GetLayerDefn()
    for i in range(layer_defn.GetFieldCount()):
        field_defn = layer_defn.GetFieldDefn(i)
        field_name = field_defn.GetName()
        field_type = field_defn.GetTypeName()
        field_width = field_defn.GetWidth()
        
        if field_type == 'String':
            properties[field_name] = f'str:{field_width}' if field_width > 0 else 'str'
        elif field_type == 'Integer':
            properties[field_name] = 'int'
        elif field_type == 'Real':
            properties[field_name] = 'float'
        else:
            properties[field_name] = field_type.lower()
    


# read all fields in all shape files in folder C:\Data\Topo50\Release62_NZ50_Shape

field_mappings = r"C:\Data\Model\lds_field_mapping_export.xlsx"
folder_path = r"C:\Data\Topo50\Release62_NZ50_Shape"
#field_mappings = r"C:\Data\Model\lds_field_mapping_export_rd.xlsx"
#folder_path = r"C:\Data\Topo50\Release62_NZ50_Shape\replaced_by_lamps"
search_path = os.path.join(folder_path, "*.shp")
all_fields = {}

for filename in glob.glob(search_path):

    with fiona.open(filename) as src:
        schema = src.schema
        
    fields = list(schema['properties'].keys())
    fields.append('geometry')
    all_fields[filename] = fields

    layer_name = os.path.basename(filename).replace('.shp', '')

    # Write schemas to JSON file
    json_output_path = os.path.join(folder_path, f"{layer_name}.json")
    with open(json_output_path, 'w') as f:
        json.dump(schema, f, indent=2)
    print(f"Exported schemas to {json_output_path}")


# Create a list to store the data
data = []

for filename, fields in all_fields.items():
    order = 1
    for field in fields:
        data.append({
            'filename': os.path.basename(filename).replace('.shp', ''),
            'field_name': field,
            'mapped_name': field,
            'order': order
        })
        order += 1

    # Create DataFrame and export to Excel
    df = pd.DataFrame(data)
    print(field_mappings)
    df.to_excel(field_mappings, index=False)
    print(f"Exported {len(data)} field mappings to {field_mappings}")

