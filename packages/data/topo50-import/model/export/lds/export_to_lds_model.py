import os
import geopandas as gpd
import pandas as pd
import json

class ExportToLDSModel:
    def __init__(self, layer_info_excel, field_mappings_excel, database, output_folder, schema_folder=None):
        self.excel_file = layer_info_excel
        self.field_mappings_excel = field_mappings_excel
        self.database = database
        self.output_folder = output_folder
        self.schema_folder = schema_folder
        self.layers_info = self.load_layers_info()
        self.field_names, self.mapped_names = self.load_field_mapping(field_mappings_excel)

    def is_macronated(self, word):
        macronated_maori_vowels = {'ā', 'ē', 'ī', 'ō', 'ū'}
        return any(char.lower() in macronated_maori_vowels for char in word)

    def remove_macrons(self, text):
        macron_map = {
            'ā': 'a', 'ē': 'e', 'ī': 'i', 'ō': 'o', 'ū': 'u',
            'Ā': 'A', 'Ē': 'E', 'Ī': 'I', 'Ō': 'O', 'Ū': 'U'
        }
        return ''.join(macron_map.get(char, char) for char in text)

    def read_source_data(self, format, layer_name, feature_type):
        layer = gpd.read_file(self.database, layer=layer_name, where=f"feature_type = '{feature_type}'")
        if format.lower() == 'gpkg':
            layer = gpd.read_file(self.database, layer=layer_name, where=f"feature_type = '{feature_type}'")
        elif format.lower() == 'parquet':
            database = os.path.join(self.database, f"{layer}.parquet")
            layer = gpd.read_parquet(database, where=f"feature_type = '{feature_type}'")
        else:
            raise ValueError(f"Unsupported format: {format}")
        return layer

    def load_layers_info(self):
        source = pd.read_excel(self.excel_file, sheet_name="Sheet1")
        layers_info = {}
        for row in source.itertuples():
            object_name = row.object_name
            shp_name = row.shp_name
            theme = row.theme
            dataset = row.dataset
            feature_type = row.feature_type
            layer_name = row.layer_name
            layer_list = [object_name, theme, feature_type, layer_name, dataset]
            layers_info[shp_name] = layer_list
        return layers_info

    def load_field_mapping(self, mapping_file):
        df = pd.read_excel(mapping_file)
        field_names = {}
        mapped_names = {}
        for row in df.itertuples(index=False):
            filename = row.filename
            field_name = row.field_name
            mapped_name = row.mapped_name
            if filename not in field_names:
                field_names[filename] = []
                mapped_names[filename] = []

            field_names[filename].append(field_name)
            mapped_names[filename].append(mapped_name)
        return field_names, mapped_names
    
    def read_schema(self, schema_file):
        with open(schema_file, 'r') as f:
            schema_dict = json.load(f)
        return schema_dict
    
    def check_names(self, mapped_names, layer, feature_type):
        #historic_site special case - only has description field. In reality all current historic_site names are not macronated = (N)
        #tree_pnt we droppped the name field so no need to check
        if feature_type == 'tree':
            return layer
        print ("Checking for macronated names...")
        layer['macronated'] = 'N'
        name_field = 'description' 
        if feature_type != 'historic_site':
            name_field = 'name'
            layer['name_ascii'] = layer['name']

        if 'group_name' in mapped_names:
            layer['grp_macron'] = 'N'
            layer['grp_ascii'] = layer['group_name']
            
        for idx, row in layer.iterrows():
            name_value = row[name_field]
            if name_value and self.is_macronated(str(name_value)):
                #print(f"Macronated name found: {name_value}")
                layer.at[idx, 'macronated'] = 'Y'
                if feature_type != 'historic_site':
                    layer.at[idx, 'name_ascii'] = self.remove_macrons(name_value)

            if 'group_name' in mapped_names:
                group_value = row['group_name']
                if group_value and self.is_macronated(str(group_value)):
                    #print(f"Macronated group name found: {group_value}")
                    layer.at[idx, 'grp_macron'] = 'Y'
                    layer.at[idx, 'grp_ascii'] = self.remove_macrons(group_value)

        return layer

    def reorder_columns(self, layer, mapped_names, field_names):
        # Drop fields not in mapped_names and rename to field_names
        # Keep only fields that are in mapped_names
        fields_to_keep = [col for col in layer.columns if col in mapped_names or col == 'geometry']
        layer = layer[fields_to_keep]

        # Create rename mapping from mapped_names to field_names
        rename_mapping = {}
        for i, mapped_name in enumerate(mapped_names):
            if mapped_name in layer.columns and i < len(field_names):
                if mapped_name != field_names[i]:
                    rename_mapping[mapped_name] = field_names[i]
        
        # Apply renaming
        if rename_mapping:
            layer = layer.rename(columns=rename_mapping)

        # Reorder columns based on field_names order
        if field_names:
            # Get geometry column separately
            geometry_col = layer.geometry.name if hasattr(layer, 'geometry') else 'geometry'
            
            # Create ordered column list: field_names + geometry
            ordered_columns = []
            for field_name in field_names:
                if field_name in layer.columns:
                    ordered_columns.append(field_name)
            
            # Add geometry column if it exists and isn't already included
            if geometry_col in layer.columns and geometry_col not in ordered_columns:
                ordered_columns.append(geometry_col)
            
            # Reorder the dataframe
            layer = layer[ordered_columns]

        return layer
    
    
    def tree_locations_special_case_remove(self, schema):
        keys_to_remove = ['macronated', 'name_ascii', 'name']
        for key in keys_to_remove:
            schema['properties'].pop(key, None)
        return schema
    
    def tree_locations_special_case(self, layer):
        keys_to_add = ['macronated', 'name_ascii', 'name']
        layer['macronated'] = 'N'
        layer['name'] = ''
        layer['name_ascii'] = ''

        # Special case for specific t50_fid values to set name fields
        fids = [4868150, 6083285, 6083286]
        for fid in fids:
            mask = layer['t50_fid'] == fid
            if mask.any():
                layer.loc[mask, 'name'] = 'Takarunga/Mount Victoria'
                layer.loc[mask, 'name_ascii'] = 'Takarunga/Mount Victoria'

        #Tuahu Kauri Tree 4731396
        mask = layer['t50_fid'] == 4731396
        if mask.any():
            layer.loc[mask, 'name'] = 'Tuahu Kauri Tree'
            layer.loc[mask, 'name_ascii'] = 'Tuahu Kauri Tree'
        
        #Waihi Golf Course 4875366
        mask = layer['t50_fid'] == 4875366
        if mask.any():
            layer.loc[mask, 'name'] = 'Waihi Golf Course'
            layer.loc[mask, 'name_ascii'] = 'Waihi Golf Course'

        #Weatherall's Trees 3703133, 3703134, 3703135
        fids = [3703133, 3703134, 3703135]
        for fid in fids:
            mask = layer['t50_fid'] == fid
            if mask.any():
                layer.loc[mask, 'name'] = "Weatherall's Trees"
                layer.loc[mask, 'name_ascii'] = "Weatherall's Trees"

        return layer
    
    def skip_unknown_nodata_layers(self, shape_name):
        #in layers_info but does not exist in data - keep skipping
        skip = {'blowhole_pnt','cattlestop_pnt','fume_cl','flume_pnt','kiln_pnt','plantation_poly'}
        if shape_name in skip:
            return True
        return False
    
    
    def process_layers(self):
        processed_layers = []
        for shp_name, layer_info in self.layers_info.items():
            start_time = pd.Timestamp.now()
            object_name, theme, feature_type, layer_name, dataset = layer_info

            if self.skip_unknown_nodata_layers(shp_name):
                print(f"Skipping unknown/no data layer: {shp_name}")
                continue

            processed_layers.append(feature_type)
            layer_name = layer_name.lower()
            shp_path = os.path.join(self.output_folder, f"{shp_name}.shp").lower()
            field_names = self.field_names.get(shp_name, [])
            mapped_names = self.mapped_names.get(shp_name, [])

            ##temp debug
            #if layer_name == 'structure_point':  
            #    continue

            #if 'structure' in layer_name:
            #    continue

            layer = gpd.read_file(self.database, layer=layer_name, where=f"feature_type = '{feature_type}'")
            if 'name' in mapped_names or feature_type == 'historic_site':
                layer = self.check_names(mapped_names, layer, feature_type)

            layer = self.reorder_columns(layer, mapped_names, field_names)
            layer.to_crs(2193, inplace=True)
            schema_file = os.path.join(self.schema_folder, f"{shp_name}.json")
            schema = self.read_schema(schema_file)
            if layer_name == 'tree_locations':
                layer = self.tree_locations_special_case(layer)
                #schema = self.tree_locations_special_case_remove(schema)
            layer.to_file(shp_path, engine='fiona', schema=schema, encoding="UTF-8")
            end_time = pd.Timestamp.now()
            print(f"Exported: {layer_name} of type {feature_type} to {shp_name}: {end_time - start_time}")


            if layer_name == 'ice':
                shp_path = shp_path.replace('ice', 'snow')
                layer.to_file(shp_path, engine='fiona',  schema=schema, encoding="UTF-8")
                print(f"Exported: {layer_name} to {shp_path}")

        print(f"Processed layers: {len(processed_layers)}")

if __name__ == "__main__":

    model_folder = r"C:\Data\Model"
    schema_folder = r"C:\Data\Topo50\Release62_NZ50_Schemas"
    layer_info_excel = os.path.join(model_folder, "layers_info.xlsx")
    field_mappings_excel = os.path.join(model_folder, "lds_field_mapping.xlsx")
    source_folder = r"C:\Data\toposource\topographic-data"
    database = os.path.join(source_folder, "topographic-data.gpkg")
    output_folder = r"C:\Data\topo50\export\lds_model"
    output_folder = r"C:\temp\export\lds_model"

    os.makedirs(output_folder, exist_ok=True)

    start_time = pd.Timestamp.now()
    exporter = ExportToLDSModel(layer_info_excel, field_mappings_excel, database, output_folder, schema_folder)
    exporter.process_layers()
    end_time = pd.Timestamp.now()
    print(f"Processing time: {end_time - start_time}")




