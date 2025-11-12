import os
import datetime
from pyogrio import read_dataframe
from pyogrio import list_layers
from pyogrio import read_info

class Database:
    def __init__(self, path):
        self.path = path

    def list_schema_tables(self):
        info = list_layers(self.path)
        return info

    def list_columns(self, layer_name):
        info = read_info(self.path, layer=layer_name)
        return info['fields']

if __name__ == "__main__":
    data_path = r"C:\Data\topoedit\topographic-data\topographic-data.gpkg"
    export_path = r"C:\Data\temp"

    release_name = 'release'
    release_date = "2025-02-05"
    export_folder = os.path.join(export_path, release_name, release_date)
    if not os.path.exists(export_folder):
        os.makedirs(export_folder)

    database = Database(data_path)
    tables = database.list_schema_tables()
    non_spatial_tables = ['collections']

    start_time = datetime.datetime.now()
    print(f"Starting processing of {len(tables)} tables at {start_time}")
    for table_info in tables:
        try:
            table = table_info[0]
            print(f"Table: {table}")

            if 'contour' not in table:
                continue

            columns = database.list_columns(table)
            fields = ", ".join(columns)
            fields = fields.replace("topo_id", "topo_id::text")
            print(fields)

            source_gdf = read_dataframe(data_path, layer=table, columns=columns, use_arrow=True)
            parquet_file = os.path.join(export_folder, f"{table}.parquet")
            print(f"Exporting to {parquet_file}")

            file_time = datetime.datetime.now()
            df = read_dataframe(data_path, layer=table, columns=columns, use_arrow=True)
    
            if table not in non_spatial_tables:
                df.to_parquet(parquet_file, engine='pyarrow', compression='zstd', compression_level=3, write_covering_bbox=True, row_group_size=50000)
            else:
                df.to_parquet(parquet_file, engine='pyarrow', compression='zstd', compression_level=3, row_group_size=50000)
            print(f"Completed {table} in {datetime.datetime.now() - file_time}")
        except Exception as e:
            error_msg = f"Error processing table {table}: {str(e)}"
            print(error_msg)

    parquet_files = [f for f in os.listdir(export_folder) if f.endswith('.parquet')]
    print(f"Completed processing all {len(tables)} tables")
    print(f"Total .parquet files created: {len(parquet_files)}")
    if len(parquet_files) == len(tables):
        print("All tables processed successfully.")
    else:
        print("Some tables failed to process.")
