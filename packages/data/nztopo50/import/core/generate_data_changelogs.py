#!/usr/bin/env python3
"""
Standalone script for comparing database table data between releases and generating changelogs.

This script extracts comparison functionality from postgis_manage_fields.py to run independently.
It compares tables between two database schemas (current and previous release) and exports
changes as parquet files for analysis.
"""

import os
import psycopg
import pandas as pd
import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore

# Database connection parameters
DB_PARAMS = {
    "dbname": "topo",
    "user": "postgres",
    "password": "landinformation",
    "host": "localhost",
    "port": 5432,
}


class TableComparator:
    """Class for comparing table data between database releases."""

    def __init__(self, db_params):
        self.db_params = db_params
        self.conn = None
        self.connect()

    def connect(self):
        """Establish database connection."""
        if self.conn is None or self.conn.closed:
            self.conn = psycopg.connect(**self.db_params)

    def close(self):
        """Close database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()

    def list_schema_tables(self, schema_name):
        """Get list of tables in a schema."""
        self.connect()
        query = f"""
        SELECT table_schema, table_name FROM information_schema.tables
        WHERE table_schema = '{schema_name}' 
        ORDER BY table_schema, table_name
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            schema_tables = {}
            for schema, table in rows:
                if schema not in schema_tables:
                    schema_tables[schema] = []
                schema_tables[schema].append(table)
        return schema_tables

    def column_exists(self, schema, table, column_name, use_like=False):
        """Check if a column exists in a table."""
        self.connect()
        if use_like:
            column_sql = f"like '%{column_name}%'"
        else:
            column_sql = f"= '{column_name}'"
        query = f"""
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{table}' AND column_name {column_sql}
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchone() is not None

    def all_ordered_columns(self, primary_key_type="int"):
        """Get all possible columns in predefined order."""
        ordered_list = [
            "id",
            "t50_fid",
            "topo_id",
            "feature_type",
            "bridge_use",
            "bridge_use2",
            "building_use",
            "elevation_use",
            "relief_use",
            "infrastructure_use",
            "landcover_use",
            "landuse_use",
            "railway_use",
            "runway_use",
            "shaft_use",
            "structure_use",
            "tunnel_use",
            "tunnel_use2",
            "track_use",
            "construction_type",
            "water_use",
            "lid_type",
            "support_type",
            "tank_type",
            "track_type",
            "landcover_type",
            "landuse_type",
            "tunnel_type",
            "vehicle_type",
            "structure_type",
            "trig_type",
            "sub_type",
            "coastline_type",
            "place_type",
            "hierarchy",
            "level",
            "species",
            "species_cultivated",
            "status",
            "name",
            "group_name",
            "nzgb_id",
            "code",
            "info_display",
            "location",
            "highway_number",
            "sheet_code",
            "sheet_name",
            "elevation",
            "composition",
            "desc_code",
            "description",
            "display",
            "definition",
            "designated",
            "designation",
            "formation",
            "edition",
            "height",
            "orientation",
            "lane_count",
            "material",
            "materials",
            "material_conveyed",
            "perennial",
            "restrictions",
            "revised",
            "size",
            "stored_item",
            "substance",
            "surface",
            "temperature",
            "temperature_indicator",
            "visibility",
            "way_count",
            "road_access",
            "width",
            "name_id",
            "wreck_of",
            "shape_area",
            "rna_sufi",
            "route",
            "route2",
            "route3",
            "collection_id",
            "collection_name",
            "theme",
            "source",
            "source_date",
            "capture_method",
            "change_type",
            "update_date",
            "create_date",
            "version",
        ]

        if primary_key_type == "uuid":
            ordered_list.remove("id")
            ordered_list.remove("topo_id")
            ordered_list.insert(0, "topo_id")
        return ordered_list

    def get_non_compare_columns(self):
        """Get columns that should not be compared when checking for differences."""
        non_compare_columns = [
            "feature_type",
            "theme",
            "source",
            "source_date",
            "capture_method",
            "change_type",
            "update_date",
            "create_date",
            "version",
        ]
        return non_compare_columns

    def get_compare_columns(self, schema, table):
        """Get columns that should be compared when checking for differences."""
        self.connect()
        non_compare_columns = self.get_non_compare_columns()
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema, table),
            )
            existing_columns = [row[0] for row in cur.fetchall()]

        # Filter and order the existing columns according to the predefined order
        compare_columns = [
            col
            for col in self.all_ordered_columns()
            if col in existing_columns and col not in non_compare_columns
        ]
        return compare_columns

    def compare_table_data(self, schema, table, previous_schema, release_date=None):
        """Compare table data between current and previous schema."""
        self.connect()
        compare_columns = self.get_compare_columns(schema, table)
        if not compare_columns:
            print(f"No comparable columns found for {schema}.{table}")
            return None

        # INSERTS - records in new schema but not in previous
        query = f"""
            SELECT b.topo_id::text, '{table}' as table_name, b.feature_type, 'added' AS change_type
            FROM {schema}.{table} AS b
            LEFT JOIN {previous_schema}.{table} AS a ON a.topo_id = b.topo_id
            WHERE a.topo_id IS NULL; 
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            insert_df = pd.DataFrame(rows, columns=columns)

        # DELETES - records in previous schema but not in new
        query = f"""
            SELECT a.topo_id::text, '{table}' as table_name, a.feature_type, 'removed' AS change_type
            FROM {previous_schema}.{table} AS a
            LEFT JOIN {schema}.{table} AS b ON a.topo_id = b.topo_id
            WHERE b.topo_id IS NULL; 
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            delete_df = pd.DataFrame(rows, columns=columns)

        # UPDATES - records that exist in both but have different values
        distinct_conditions = " OR ".join(
            [f"a.{col} IS DISTINCT FROM b.{col}" for col in compare_columns]
        )
        query = f"""
            SELECT a.topo_id::text, '{table}' as table_name, a.feature_type, 'updated' AS change_type
            FROM {previous_schema}.{table} AS a
            JOIN {schema}.{table} AS b ON a.topo_id = b.topo_id
            WHERE {distinct_conditions};
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            update_df = pd.DataFrame(rows, columns=columns)

        # Add release date columns if provided
        if release_date:
            year, month, day = release_date.split("-")
            for df in [insert_df, delete_df, update_df]:
                df["year"] = year
                df["month"] = month
                df["day"] = day

        return insert_df, delete_df, update_df


def main():
    """Main comparison logic for generating changelogs."""

    # Configuration
    comparator = TableComparator(DB_PARAMS)
    schema_name = "release64"
    previous_schema = "release62"
    release_date = "2025-09-25"

    # Output settings
    use_hive_partitioning = False
    change_logs_path = r"c:\data\topo-data-vector\changelogs"

    # Create output directory
    if not os.path.exists(change_logs_path):
        os.makedirs(change_logs_path)

    # Get list of tables to compare
    schema_tables = comparator.list_schema_tables(schema_name)

    # Compare tables and generate changelogs
    df_order = ["added", "removed", "updated"]
    i = 0
    all_dfs = None

    for schema, tables in schema_tables.items():
        for table in tables:
            if table == "collections":
                continue

            print(f"Comparing table: {schema}.{table}")
            dfs = comparator.compare_table_data(
                schema, table, previous_schema, release_date
            )
            i += 1

            if dfs is None:
                print(
                    f"No comparable columns for {schema}.{table}, skipping comparison."
                )
                continue

            insert_df, delete_df, update_df = dfs

            # Concatenate results from all tables
            if i == 1:
                all_dfs = [insert_df, delete_df, update_df]
            else:
                all_dfs[0] = pd.concat([all_dfs[0], insert_df], ignore_index=True)
                all_dfs[1] = pd.concat([all_dfs[1], delete_df], ignore_index=True)
                all_dfs[2] = pd.concat([all_dfs[2], update_df], ignore_index=True)

    # Export results to parquet files
    if all_dfs:
        for i, df in enumerate(all_dfs):
            change_type = df_order[i]

            if df.empty:
                print(f"No {change_type} changes detected")
            else:
                if not use_hive_partitioning:
                    # Standard parquet output
                    output_file = os.path.join(
                        change_logs_path,
                        schema_name,
                        f"{schema_name}_{change_type}_changelog.parquet",
                    )
                    os.makedirs(os.path.dirname(output_file), exist_ok=True)
                    df.to_parquet(output_file, index=False)
                    print(f"Exported {change_type} changes to {output_file}")
                else:
                    # Hive partitioned output
                    partition_path = os.path.join(change_logs_path, schema_name)
                    os.makedirs(partition_path, exist_ok=True)
                    partition_path = partition_path.replace("\\", "/")

                    pq.write_to_dataset(
                        pa.Table.from_pandas(df),
                        root_path=partition_path,
                        partition_cols=["year", "month", "day", "change_type"],
                    )
                    print(
                        f"Exported {change_type} changes with partitioning to {partition_path}"
                    )

    # Close database connection
    comparator.close()
    print("Comparison complete!")


if __name__ == "__main__":
    main()
