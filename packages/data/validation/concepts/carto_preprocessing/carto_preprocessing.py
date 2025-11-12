import os
import glob
import geopandas as gpd
import shutil

def get_cartographic_classification_fields():
        update_dict = {
            ("structure_point"): [("class_use","feature_type", "structure_use"),
                                  ("class_type","feature_type","structure_type")],
            ("water"): [("class_type","feature_type","water_use")],
        }
        return list(update_dict.keys()), update_dict

if __name__ == "__main__":

    #\\PRDASSFILE01.ad.linz.govt.nz\dfs\sites\LH\Group
    data_path = r"\\PRDASSFILE01.ad.linz.govt.nz\dfs\sites\LH\Group\Share\AMcMenamin\release62_parquet\2025-02-05"
    search_path = os.path.join(data_path, "*.parquet")
    output_path = r"C:\Data\temp"

    layers, carto_fields = get_cartographic_classification_fields()

    for file in glob.glob(search_path):
        layer_name = os.path.basename(file).replace('.parquet','')
        if layer_name in layers:
            print(f"Adding Classification Fields to {layer_name}")

            gdf = gpd.read_parquet(file)

            for new_field, base_field, combine_field in carto_fields[layer_name]:
                
                gdf[new_field] = gdf[base_field].fillna('').astype(str) + ' ' + gdf[combine_field].fillna('').astype(str)
            
            output_file = os.path.join(output_path, f"{layer_name}.parquet")

            gdf.to_parquet(output_file, engine='pyarrow', compression='zstd',
                        compression_level=3, write_covering_bbox=True, row_group_size=50000)
            print(f"Updated {file} with cartographic classification fields")

        else:
            output_file = os.path.join(output_path, f"{layer_name}.parquet")
           
            # Generic copy: just copy the file as-is
            shutil.copy2(file, output_file)
            print(f"Copied {file} to {output_file} without modification")


