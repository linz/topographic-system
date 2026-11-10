"""Create SQL statements for grouped column-content checks (Step 1).

This script reads schema_columns_groupby.csv, keeps rows marked for grouping,
and generates one SELECT/GROUP BY statement per table+column combination. The
output SQL file is used as the first step before executing statements and
reviewing per-column values.
"""

import psycopg
import csv
from pathlib import Path

# Database connection parameters
DB_PARAMS = {
    'dbname': 'topo',
    'user': 'postgres',
    'password': 'landinformation',
    'host': 'localhost',
    'port': 5432
}


if __name__ == "__main__":

    schema_name = "release66"

    source_file = Path(r"C:\Data\model\schema_columns_groupby.csv")
    output_file = source_file.with_name("schema_columns_groupby_queries.sql")

    statements = []

    with source_file.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for row in reader:
            table_name = (row.get("table") or "").strip()
            column_name = (row.get("columns") or "").strip()
            group_by_value = (row.get("group_by") or "").strip().upper()

            if not table_name or not column_name:
                continue

            if group_by_value not in {"TRUE", "T", "1", "YES", "Y"}:
                continue

            if "." in table_name:
                table_ref = table_name
            else:
                table_ref = f"{schema_name}.{table_name}" if schema_name else table_name

            statement = (
                f"SELECT type, {column_name} FROM {table_ref} "
                f"GROUP BY type, {column_name} "
                f"ORDER BY {column_name};"
            )
            statements.append(statement)

    with output_file.open("w", encoding="utf-8") as f:
        f.write("\n".join(statements))
        f.write("\n")

    print(f"Wrote {len(statements)} SQL statements to {output_file}") 