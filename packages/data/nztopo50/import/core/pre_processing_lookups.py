from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import psycopg
import pyogrio
from sqlalchemy import create_engine


db_params: dict[str, Any] = {
    "dbname": "topo",
    "user": "postgres",
    "password": "landinformation",
    "host": "localhost",
    "port": 5432,
}


def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
    conn.commit()


def drop_table_if_exists(
    conn: psycopg.Connection,
    schema: str,
    table: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table.lower()}";')
    conn.commit()


def create_t50_fid_index(
    conn: psycopg.Connection,
    schema: str,
    table: str,
) -> None:
    table_name = table.lower()
    index_name = f"{table_name}_t50_fid_idx"
    with conn.cursor() as cur:
        cur.execute(
            f'CREATE INDEX IF NOT EXISTS "{index_name}" '
            f'ON "{schema}"."{table_name}" (t50_fid);'
        )
    conn.commit()


def load_roads_shp(
    shp_path: str,
    schema: str,
    table: str,
    if_exists: str,
    params: dict[str, Any],
) -> None:
    input_path = Path(shp_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Shapefile not found: {input_path}")

    # Only load the required fields (no geometry).
    keep_fields = ["UFID", "name", "road_acces", "width", "name_id","lol_sufi"]
    df = pyogrio.read_dataframe(input_path, columns=keep_fields, read_geometry=False)

    # Rename source fields to target names and lower-case all column names.
    df = df.rename(
        columns={
            "UFID": "t50_fid",
            "road_acces": "road_access",
            "width": "width_indicator",
        }
    )
    df.columns = [str(col).lower() for col in df.columns]

    # Convert name_id to integer, keeping NaN for missing values
    df["name_id"] = pd.to_numeric(df["name_id"], errors="coerce").astype("Int64")

    engine = create_engine(
        (
            f"postgresql+psycopg://{params['user']}:{params['password']}"
            f"@{params['host']}:{params['port']}/{params['dbname']}"
        )
    )

    with psycopg.connect(**params) as conn:
        ensure_schema(conn, schema)
        drop_table_if_exists(conn, schema, table)

    df.to_sql(
        name=table.lower(),
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
    )

    with psycopg.connect(**params) as conn:
        create_t50_fid_index(conn, schema, table)

    print(
        f"Loaded {len(df)} rows from '{input_path.name}' into "
        f"{schema}.{table.lower()}"
    )


if __name__ == "__main__":
    # Input file and target table settings.
    shp_path = r"C:\Data\Topo50\lookups\road_cl.shp"
    schema = "lookups"
    table = "road_lkp"
    if_exists = "replace"  # fail | replace | append

    # Database settings.
    run_params = {
        "dbname": "topo",
        "user": "postgres",
        "password": "landinformation",
        "host": "localhost",
        "port": 5432,
    }

    load_roads_shp(
        shp_path=shp_path,
        schema=schema,
        table=table,
        if_exists=if_exists,
        params=run_params,
    )