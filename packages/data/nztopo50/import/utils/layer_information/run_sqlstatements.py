"""Run SQL statements from a file and export each result set to CSV.

This script reads semicolon-separated SQL queries from
schema_columns_groupby_queries.sql, executes them against Postgres, and writes
one CSV file per statement in the sql_results folder. Output filenames are
derived from SELECT columns and table names where possible, with a fallback to
indexed names when needed.
"""

import os
from pathlib import Path
import psycopg
import csv
import re
from psycopg import sql


# Database connection parameters
DB_PARAMS = {
    'dbname': 'topo',
    'user': 'postgres',
    'password': 'landinformation',
    'host': 'localhost',
    'port': 5432
}


def build_filename_from_select(statement: str, fallback_index: int) -> str:
    select_match = re.search(r"^\s*SELECT\s+(.*?)\s+FROM\s", statement, flags=re.IGNORECASE | re.DOTALL)
    from_match = re.search(r"\bFROM\s+([^\s;]+)", statement, flags=re.IGNORECASE)

    if not select_match:
        return f"statement_{fallback_index:03d}.csv"

    select_part = select_match.group(1)
    raw_columns = [part.strip() for part in select_part.split(",") if part.strip()]

    cleaned_columns = []
    for col in raw_columns:
        # Keep the selected expression name simple for filesystem-safe filenames.
        col_name = re.split(r"\s+AS\s+", col, flags=re.IGNORECASE)[-1]
        col_name = col_name.strip().strip('"')
        col_name = re.sub(r"[^0-9A-Za-z_]+", "_", col_name)
        col_name = re.sub(r"_+", "_", col_name).strip("_").lower()
        if col_name:
            cleaned_columns.append(col_name)

    table_name = ""
    if from_match:
        table_name = from_match.group(1).strip().strip('"')
        table_name = re.sub(r"[^0-9A-Za-z_]+", "_", table_name)
        table_name = re.sub(r"_+", "_", table_name).strip("_").lower()

    filename_parts = []
    if table_name:
        filename_parts.append(table_name)
    filename_parts.extend(cleaned_columns)

    if not filename_parts:
        return f"statement_{fallback_index:03d}.csv"

    return f"{'_'.join(filename_parts)}.csv"


if __name__ == "__main__":

    schema_name = "release66"

    work_folder = Path(r"C:\Data\model")

    sql_file = os.path.join(work_folder, "schema_columns_groupby_queries.sql")
    output_folder = os.path.join(work_folder, "sql_results")
    os.makedirs(output_folder, exist_ok=True)

    with open(sql_file, "r", encoding="utf-8") as f:
        sql_text = f.read()

    statements = [
        stmt.strip()
        for stmt in sql_text.split(";")
        if stmt.strip()
    ]

    with psycopg.connect(**DB_PARAMS) as conn:
        with conn.cursor() as cur:
            if schema_name:
                cur.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema_name)))

            for idx, statement in enumerate(statements, start=1):
                cur.execute(statement)
                rows = cur.fetchall()

                headers = [desc.name for desc in cur.description] if cur.description else []

                csv_name = build_filename_from_select(statement, idx)
                csv_path = os.path.join(output_folder, csv_name)

                if os.path.exists(csv_path):
                    stem, ext = os.path.splitext(csv_name)
                    csv_name = f"{stem}_{idx:03d}{ext}"
                    csv_path = os.path.join(output_folder, csv_name)

                with open(csv_path, "w", newline="", encoding="utf-8") as csv_file:
                    writer = csv.writer(csv_file)
                    if headers:
                        writer.writerow(headers)
                    writer.writerows(rows)

                print(f"[{idx}/{len(statements)}] Wrote {csv_path} ({len(rows)} rows)")

    print(f"Completed {len(statements)} statements. Output folder: {output_folder}")

    