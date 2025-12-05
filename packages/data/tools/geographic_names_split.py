import os
import geopandas as gpd
import pandas as pd

# We are trying to match NZGB gazeeteer names to Topo50 geographic names
# The names are not unqiue so looking for proxmity currently 1km
# then trying to match on name and export

gaz_db = r"C:\Data\Topo50\nzgb_gaz\gaz.gpkg"
gaz_layer = "nzgb_gaz"
gaz_name_field = 'name'

topo_db = r"C:\Data\topoedit\topographic-data\topographic-data.gpkg"
topo_layer = "geographic_name"
topo_name_field = 'name'

output_db = r"C:\temp\geographic_names_split.gpkg"
output_layer = "geographic_names_match"

# Delete output database if it exists
if os.path.exists(output_db):
    os.remove(output_db)

# Read the layers using geopandas
gaz_data = gpd.read_file(gaz_db, layer=gaz_layer)
topo_data = gpd.read_file(topo_db, layer=topo_layer)

# Project both layers to NZGD2000 / New Zealand Transverse Mercator 2000 (EPSG:2193)
gaz_data = gaz_data.to_crs(2193)
topo_data = topo_data.to_crs(2193)

# Buffer gaz_data by 1000 meters
gaz_data_buffered = gaz_data.buffer(1000)

# Perform spatial join to find matches within 1000 meters
spatial_matched = gpd.sjoin(topo_data, gpd.GeoDataFrame(geometry=gaz_data_buffered), how="left", predicate='intersects')

# Join the spatial matches with the original gazetteer data
spatial_matched = spatial_matched.merge(gaz_data, left_on='index_right', right_index=True, how='left', suffixes=('', '_gaz'))
# Drop the 'geometry_gaz' field
spatial_matched = spatial_matched.drop(columns=['geometry_gaz'])

spatial_matched['unmatched_names'] = 0
spatial_matched.loc[(spatial_matched['name'] != spatial_matched['name_gaz']) & (spatial_matched['index_right'].notna()), 'unmatched_names'] = 1

# Reset the index to ensure continuous indexing
spatial_matched = spatial_matched.reset_index(drop=True)

# Get unique desc_code values and print them
unique_desc_codes = sorted(spatial_matched['desc_code'].unique())
print("Unique desc_code values:")
for code in unique_desc_codes:
    print(f"  {code}")

unique_desc_codes = sorted(spatial_matched['feat_type'].unique().astype(str))
print("Unique feat_type values:")
for code in unique_desc_codes:
    print(f"  {code}")

# Loop through records where unmatched_names = 1 and check if name_gaz contains name
for idx, row in spatial_matched[spatial_matched['unmatched_names'] == 1].iterrows():
    if pd.notna(row['name_gaz']) and pd.notna(row['name']):
        if row['name'].lower() in row['name_gaz'].lower():
            spatial_matched.at[idx, 'unmatched_names'] = 2


# Save the matched records to a new GeoPackage
spatial_matched.to_file(output_db, layer=output_layer, driver='GPKG')

print(f"Matched geographic names saved to {output_db} in layer {output_layer}")