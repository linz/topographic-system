#!/usr/bin/env python3
"""
Standalone script for comparing GeoParquet table data between releases and generating changelogs.

This DuckDB-based alternative reads source GeoParquet files from two release folders
instead of querying Postgres schemas.
"""

import glob
import os
from typing import Any

import duckdb
import pandas as pd
import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore


class GeoParquetTableComparator:
    """Compare table data between two release folders of parquet/geoparquet files."""

    def __init__(self):
        self.conn = duckdb.connect()

    def _is_s3_path(self, path: str) -> bool:
        return path.lower().startswith("s3://")

    def configure_s3_from_env(self):
        """Enable DuckDB S3 access using optional AWS-style environment variables."""
        self.conn.execute("INSTALL httpfs")
        self.conn.execute("LOAD httpfs")

        env_to_setting = {
            "AWS_REGION": "s3_region",
            "AWS_DEFAULT_REGION": "s3_region",
            "AWS_ACCESS_KEY_ID": "s3_access_key_id",
            "AWS_SECRET_ACCESS_KEY": "s3_secret_access_key",
            "AWS_SESSION_TOKEN": "s3_session_token",
            "AWS_ENDPOINT_URL": "s3_endpoint",
        }

        for env_key, setting in env_to_setting.items():
            value = os.getenv(env_key)
            if value:
                escaped_value = value.replace("'", "''")
                self.conn.execute(f"SET {setting}='{escaped_value}'")

    def close(self):
        self.conn.close()

    def list_release_tables(self, release_path: str) -> dict[str, str]:
        """Return a map of table_name -> parquet file path for one release folder."""
        if self._is_s3_path(release_path):
            search_path = release_path.rstrip("/") + "/*.parquet"
            files = [row[0] for row in self.conn.execute("SELECT file FROM glob(?)", [search_path]).fetchall()]
        else:
            files = glob.glob(os.path.join(release_path, "*.parquet"))

        table_files = {}
        for file_path in files:
            table_name = os.path.splitext(os.path.basename(file_path))[0]
            table_files[table_name] = file_path
        return table_files

    def all_ordered_columns(self, primary_key_type: str = "int") -> list[str]:
        """Get all possible columns in predefined order."""
        ordered_list = [
            "id", "t50_fid", "topo_id", "feature_type", "bridge_use", "bridge_use2",
            "building_use", "elevation_use", "relief_use", "infrastructure_use",
            "landcover_use", "landuse_use", "railway_use", "runway_use", "shaft_use",
            "structure_use", "tunnel_use", "tunnel_use2", "track_use", "construction_type",
            "water_use", "lid_type", "support_type", "tank_type", "track_type",
            "landcover_type", "landuse_type", "tunnel_type", "vehicle_type", "structure_type",
            "trig_type", "sub_type", "coastline_type", "place_type", "hierarchy", "level",
            "species", "species_cultivated", "status", "name", "group_name", "nzgb_id",
            "code", "info_display", "location", "highway_number", "sheet_code", "sheet_name",
            "elevation", "composition", "desc_code", "description", "display", "definition",
            "designated", "designation", "formation", "edition", "height", "orientation",
            "lane_count", "material", "materials", "material_conveyed", "perennial",
            "restrictions", "revised", "size", "stored_item", "substance", "surface",
            "temperature", "temperature_indicator", "visibility", "way_count", "road_access",
            "width", "name_id", "wreck_of", "shape_area", "rna_sufi", "route", "route2",
            "route3", "collection_id", "collection_name", "theme", "source", "source_date",
            "capture_method", "change_type", "update_date", "create_date", "version",
        ]

        if primary_key_type == "uuid":
            ordered_list.remove("id")
            ordered_list.remove("topo_id")
            ordered_list.insert(0, "topo_id")
        return ordered_list

    def get_non_compare_columns(self) -> list[str]:
        """Columns excluded from update comparison."""
        return [
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

    def get_table_columns(self, parquet_path: str) -> list[str]:
        """Get parquet column names using DuckDB."""
        query = "SELECT * FROM read_parquet(?) LIMIT 0"
        result_df = self.conn.execute(query, [parquet_path]).fetchdf()
        return list(result_df.columns)

    def _quote_ident(self, identifier: str) -> str:
        """DuckDB-safe quoted identifier."""
        return '"' + identifier.replace('"', '""') + '"'

    def get_compare_columns(self, current_parquet: str, previous_parquet: str) -> list[str]:
        """Get ordered comparable columns present in both parquet files."""
        non_compare_columns = self.get_non_compare_columns()
        current_columns = set(self.get_table_columns(current_parquet))
        previous_columns = set(self.get_table_columns(previous_parquet))
        common_columns = current_columns.intersection(previous_columns)

        compare_columns = [
            col
            for col in self.all_ordered_columns()
            if col in common_columns and col not in non_compare_columns
        ]
        return compare_columns

    def compare_table_data(
        self,
        table: str,
        current_parquet: str,
        previous_parquet: str,
        release_date: str | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Compare one table between current and previous release parquet files."""
        current_columns = set(self.get_table_columns(current_parquet))
        previous_columns = set(self.get_table_columns(previous_parquet))

        if "topo_id" not in current_columns or "topo_id" not in previous_columns:
            empty = pd.DataFrame(columns=["topo_id", "table_name", "feature_type", "change_type"])
            return empty, empty.copy(), empty.copy()

        # INSERTS - records in current release but not in previous
        query_insert = """
            SELECT
                CAST(b.topo_id AS VARCHAR) AS topo_id,
                ? AS table_name,
                b.feature_type,
                'added' AS change_type
            FROM read_parquet(?) AS b
            LEFT JOIN read_parquet(?) AS a ON a.topo_id = b.topo_id
            WHERE a.topo_id IS NULL
        """
        insert_df = self.conn.execute(query_insert, [table, current_parquet, previous_parquet]).fetchdf()

        # DELETES - records in previous release but not in current
        query_delete = """
            SELECT
                CAST(a.topo_id AS VARCHAR) AS topo_id,
                ? AS table_name,
                a.feature_type,
                'removed' AS change_type
            FROM read_parquet(?) AS a
            LEFT JOIN read_parquet(?) AS b ON a.topo_id = b.topo_id
            WHERE b.topo_id IS NULL
        """
        delete_df = self.conn.execute(query_delete, [table, previous_parquet, current_parquet]).fetchdf()

        # UPDATES - records in both releases with changed comparable fields
        compare_columns = self.get_compare_columns(current_parquet, previous_parquet)
        compare_columns = [col for col in compare_columns if col != "topo_id"]

        if compare_columns:
            distinct_conditions = " OR ".join(
                [
                    f"a.{self._quote_ident(col)} IS DISTINCT FROM b.{self._quote_ident(col)}"
                    for col in compare_columns
                ]
            )
            query_update = f"""
                SELECT
                    CAST(a.topo_id AS VARCHAR) AS topo_id,
                    ? AS table_name,
                    a.feature_type,
                    'updated' AS change_type
                FROM read_parquet(?) AS a
                JOIN read_parquet(?) AS b ON a.topo_id = b.topo_id
                WHERE {distinct_conditions}
            """
            update_df = self.conn.execute(query_update, [table, previous_parquet, current_parquet]).fetchdf()
        else:
            update_df = pd.DataFrame(columns=["topo_id", "table_name", "feature_type", "change_type"])

        if release_date:
            year, month, day = release_date.split("-")
            for df in [insert_df, delete_df, update_df]:
                if not df.empty:
                    df["year"] = year
                    df["month"] = month
                    df["day"] = day

        return insert_df, delete_df, update_df


def main():
    """Main comparison logic for generating changelogs from GeoParquet sources."""

    # Configuration
    current_release_name = "release64"
    previous_release_name = "release62"
    release_date = "2025-09-25"

    # Source release folders containing one parquet/geoparquet file per table.
    # Can be local paths or S3 prefixes (s3://bucket/path/releaseXX).
    # Example:
    # - C:/Data/temp/2025-09-25/release64/*.parquet
    # - C:/Data/temp/2025-02-05/release62/*.parquet
    # - s3://my-bucket/topo/2025-09-25/release64/*.parquet
    # - s3://my-bucket/topo/2025-02-05/release62/*.parquet
    current_release_path = r"C:/Data/temp/2025-09-25/release64"
    previous_release_path = r"C:/Data/temp/2025-02-05/release62"

    # Output settings
    use_hive_partitioning = False
    change_logs_path = r"c:/data/topo-data-vector/changelogs"

    comparator = GeoParquetTableComparator()

    try:
        if comparator._is_s3_path(current_release_path) or comparator._is_s3_path(previous_release_path):
            comparator.configure_s3_from_env()

        if not os.path.exists(change_logs_path):
            os.makedirs(change_logs_path)

        current_tables = comparator.list_release_tables(current_release_path)
        previous_tables = comparator.list_release_tables(previous_release_path)

        common_tables = sorted(set(current_tables).intersection(previous_tables))
        common_tables = [table for table in common_tables if table != "collections"]

        if not common_tables:
            raise RuntimeError("No common parquet tables found between current and previous release folders")

        df_order = ["added", "removed", "updated"]
        all_dfs: list[Any] | None = None

        for idx, table in enumerate(common_tables, start=1):
            print(f"Comparing table: {table}")
            insert_df, delete_df, update_df = comparator.compare_table_data(
                table=table,
                current_parquet=current_tables[table],
                previous_parquet=previous_tables[table],
                release_date=release_date,
            )

            if idx == 1:
                all_dfs = [insert_df, delete_df, update_df]
            else:
                all_dfs[0] = pd.concat([all_dfs[0], insert_df], ignore_index=True)
                all_dfs[1] = pd.concat([all_dfs[1], delete_df], ignore_index=True)
                all_dfs[2] = pd.concat([all_dfs[2], update_df], ignore_index=True)

        if all_dfs:
            for i, df in enumerate(all_dfs):
                change_type = df_order[i]
                if df.empty:
                    print(f"No {change_type} changes detected")
                    continue

                if not use_hive_partitioning:
                    output_file = os.path.join(
                        change_logs_path,
                        current_release_name,
                        f"{current_release_name}_{change_type}_changelog.parquet",
                    )
                    os.makedirs(os.path.dirname(output_file), exist_ok=True)
                    df.to_parquet(output_file, index=False)
                    print(f"Exported {change_type} changes to {output_file}")
                else:
                    partition_path = os.path.join(change_logs_path, current_release_name)
                    os.makedirs(partition_path, exist_ok=True)
                    partition_path = partition_path.replace("\\", "/")
                    pq.write_to_dataset(
                        pa.Table.from_pandas(df),
                        root_path=partition_path,
                        partition_cols=["year", "month", "day", "change_type"],
                    )
                    print(f"Exported {change_type} changes with partitioning to {partition_path}")

        print("Comparison complete!")
    finally:
        comparator.close()


if __name__ == "__main__":
    main()
