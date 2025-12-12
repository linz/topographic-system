import os
from pyogrio import read_dataframe, list_layers, read_info  # type: ignore
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
from functools import partial
import datetime


# NEEDS MORE WORK - CURRENT SEEMS TO FAIL ON 2 FILES EACH TIME. CONTOURS ALWAYS ONE.
class Database:
    def __init__(self, path):
        self.path = path

    def list_schema_tables(self):
        info = list_layers(self.path)
        return info

    def list_columns(self, layer_name):
        info = read_info(self.path, layer=layer_name)
        return info["fields"]


def process_table_to_parquet(table_info, data_path, export_folder, non_spatial_tables):
    """
    Process a single table: read from GPKG and export to Parquet
    This function is designed to be run in parallel
    """
    table = table_info[0]
    print(f"Processing table: {table}")

    try:
        # Get column information
        info = read_info(data_path, layer=table)
        columns = info["fields"]
        fields = ", ".join(columns)
        fields = fields.replace("topo_id", "topo_id::text")
        print(f"Table: {table} - Fields: {fields}")

        # Read dataframe
        print(f"Reading data for table: {table}")
        df = read_dataframe(data_path, layer=table, columns=columns, use_arrow=True)

        # Export to parquet
        parquet_file = os.path.join(export_folder, f"{table}.parquet")
        print(f"Exporting {table} to {parquet_file}")

        if table not in non_spatial_tables:
            df.to_parquet(
                parquet_file,
                engine="pyarrow",
                compression="zstd",
                compression_level=3,
                write_covering_bbox=True,
                row_group_size=50000,
            )
        else:
            df.to_parquet(
                parquet_file,
                engine="pyarrow",
                compression="zstd",
                compression_level=3,
                row_group_size=50000,
            )

        print(f"Completed processing table: {table}")
        return f"Success: {table}"

    except Exception as e:
        error_msg = f"Error processing table {table}: {str(e)}"
        print(error_msg)
        return error_msg


if __name__ == "__main__":
    data_path = r"C:\Data\topoedit\topographic-data\topographic-data.gpkg"
    export_path = r"C:\Data\temp"
    start_time = datetime.datetime.now()

    release_name = "release"
    release_date = "2025-02-05"
    export_folder = os.path.join(export_path, release_name, release_date)
    if not os.path.exists(export_folder):
        os.makedirs(export_folder)

    database = Database(data_path)
    tables = database.list_schema_tables()
    non_spatial_tables = ["collections"]

    print(f"Found {len(tables)} tables to process")
    print(f"Using {multiprocessing.cpu_count()} CPU cores")

    # Create a partial function with fixed arguments
    process_func = partial(
        process_table_to_parquet,
        data_path=data_path,
        export_folder=export_folder,
        non_spatial_tables=non_spatial_tables,
    )

    # Process tables in parallel
    max_workers = min(
        round(multiprocessing.cpu_count() / 2), len(tables)
    )  # Don't use more workers than tables

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_table = {
            executor.submit(process_func, table_info): table_info[0]
            for table_info in tables
        }

        # Process completed tasks
        completed_count = 0
        total_tables = len(tables)

        for future in as_completed(future_to_table):
            table_name = future_to_table[future]
            completed_count += 1

            try:
                result = future.result()
                print(f"[{completed_count}/{total_tables}] {result}")
            except Exception as exc:
                print(
                    f"[{completed_count}/{total_tables}] Table {table_name} generated an exception: {exc}"
                )

    parquet_files = [f for f in os.listdir(export_folder) if f.endswith(".parquet")]
    print(f"Total .parquet files in export folder: {len(parquet_files)}")
    print(f"Completed processing all {len(tables)} tables")
    if len(parquet_files) == len(tables):
        print("All tables processed successfully.")
    else:
        print("Some tables failed to process.")
    print(f"Total time taken: {datetime.datetime.now() - start_time}")
