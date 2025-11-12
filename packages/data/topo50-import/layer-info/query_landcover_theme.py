import os
import pandas as pd

folder = r"C:\Data\Model"
output_path = r"c:\temp\landcover.xlsx"
layer_info_path = os.path.join(folder, "layers_info.xlsx")


df = pd.read_excel(layer_info_path)
distinct_rows = df[['theme', 'feature_type', 'layer_name']].drop_duplicates()
distinct_rows = distinct_rows.sort_values(['theme', 'feature_type', 'layer_name'])

distinct_rows = distinct_rows[distinct_rows['theme'] != 'Relief']
distinct_rows = distinct_rows[distinct_rows['theme'] != 'MapSheets']
distinct_rows = distinct_rows[distinct_rows['theme'] != 'Annonation']

distinct_rows = distinct_rows[~distinct_rows['layer_name'].str.contains('_line|_point', na=False)]

distinct_rows['class'] = distinct_rows['theme']
distinct_rows.loc[distinct_rows['theme'] == 'Buildings', 'class'] = 'landcover'
distinct_rows.loc[distinct_rows['theme'] == 'Transport', 'class'] = 'landuse'
distinct_rows.loc[distinct_rows['theme'] == 'Infrastructure', 'class'] = 'landuse'


distinct_rows = distinct_rows.sort_values(['class', 'layer_name', 'feature_type'])
distinct_rows = distinct_rows[['class', 'feature_type', 'layer_name', 'theme']]

with pd.ExcelWriter(output_path) as writer:
    distinct_rows.to_excel(writer, sheet_name='themes-layers', index=False, header=['Class', 'Feature_Type', 'Layer_Name', 'Theme'])

