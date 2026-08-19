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


def drop_table_if_exists(conn: psycopg.Connection, schema: str, table: str) -> None:
	with conn.cursor() as cur:
		cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table.lower()}";')
	conn.commit()


def create_indexes(conn: psycopg.Connection, schema: str, table: str) -> None:
	table_name = table.lower()
	with conn.cursor() as cur:
		cur.execute(
			f'CREATE INDEX IF NOT EXISTS "{table_name}_topo_id_idx" '
			f'ON "{schema}"."{table_name}" (topo_id);'
		)
		cur.execute(
			f'CREATE INDEX IF NOT EXISTS "{table_name}_t50_fid_idx" '
			f'ON "{schema}"."{table_name}" (t50_fid);'
		)
		cur.execute(
			f'CREATE INDEX IF NOT EXISTS "{table_name}_layer_name_idx" '
			f'ON "{schema}"."{table_name}" (layer_name);'
		)
	conn.commit()


def _find_column_name(columns: list[str], candidates: list[str]) -> str | None:
	col_map = {str(col).lower(): str(col) for col in columns}
	for candidate in candidates:
		hit = col_map.get(candidate.lower())
		if hit is not None:
			return hit
	return None


def extract_topoid_fields_from_layer(gpkg_path: Path, layer_name: str) -> pd.DataFrame:
	layer_df = pyogrio.read_dataframe(gpkg_path, layer=layer_name, read_geometry=False)
	columns = [str(col) for col in layer_df.columns]

	topo_id_col = _find_column_name(columns, ["topo_id", "id"])
	t50_fid_col = _find_column_name(columns, ["t50_fid"])
	type_col = _find_column_name(columns, ["type"])
	created_at_col = _find_column_name(columns, ["created_at", "create_date"])

	missing = []
	if topo_id_col is None:
		missing.append("topo_id/id")
	if t50_fid_col is None:
		missing.append("t50_fid")
	if type_col is None:
		missing.append("type")
	if created_at_col is None:
		missing.append("created_at/create_date")

	if missing:
		print(
			f"Skipping layer '{layer_name}' - missing required fields: {', '.join(missing)}"
		)
		return pd.DataFrame(
			columns=["topo_id", "t50_fid", "type", "created_at", "layer_name"]
		)

	out = pd.DataFrame(
		{
			"topo_id": layer_df[topo_id_col],
			"t50_fid": layer_df[t50_fid_col],
			"type": layer_df[type_col],
			"created_at": layer_df[created_at_col],
			"layer_name": layer_name,
		}
	)

	out["topo_id"] = out["topo_id"].astype("string")
	out["type"] = out["type"].astype("string")
	out["created_at"] = pd.to_datetime(out["created_at"], errors="coerce")
	out["layer_name"] = out["layer_name"].astype("string")
	out["t50_fid"] = pd.to_numeric(out["t50_fid"], errors="coerce").astype("Int64")

	return out


def load_topoid_lookup_from_gpkg(
	gpkg_path: str,
	schema: str,
	table: str,
	if_exists: str,
	params: dict[str, Any],
) -> None:
	input_path = Path(gpkg_path)
	if not input_path.exists():
		raise FileNotFoundError(f"GPKG not found: {input_path}")

	layers = pyogrio.list_layers(input_path)
	if len(layers) == 0:
		raise ValueError(f"No layers found in: {input_path}")

	all_rows: list[pd.DataFrame] = []
	for layer in layers:
		layer_name = str(layer[0])
		print(f"Processing layer: {layer_name}")
		df = extract_topoid_fields_from_layer(input_path, layer_name)
		if not df.empty:
			all_rows.append(df)

	if not all_rows:
		raise ValueError(
			"No rows extracted. None of the layers contained required fields."
		)

	lookup_df = pd.concat(all_rows, ignore_index=True)

	engine = create_engine(
		(
			f"postgresql+psycopg://{params['user']}:{params['password']}"
			f"@{params['host']}:{params['port']}/{params['dbname']}"
		)
	)

	with psycopg.connect(**params) as conn:
		ensure_schema(conn, schema)
		if if_exists == "replace":
			drop_table_if_exists(conn, schema, table)

	lookup_df.to_sql(
		name=table.lower(),
		con=engine,
		schema=schema,
		if_exists=if_exists,
		index=False,
	)

	with psycopg.connect(**params) as conn:
		create_indexes(conn, schema, table)

	print(
		f"Loaded {len(lookup_df)} rows from '{input_path.name}' into {schema}.{table.lower()}"
	)


if __name__ == "__main__":
	gpkg_path = r"C:\Data\topoedit\topographic-data\topographic-data.gpkg"
	# gpkg_path = r"C:\Data\topoedit\topographic-contour-data\topographic-contour-data.gpkg"
	schema = "lookups"
	table = "topoid_lkp"
	if_exists = "append"  # fail | replace | append

	run_params = {
		"dbname": "topo",
		"user": "postgres",
		"password": "landinformation",
		"host": "localhost",
		"port": 5432,
	}

	load_topoid_lookup_from_gpkg(
		gpkg_path=gpkg_path,
		schema=schema,
		table=table,
		if_exists=if_exists,
		params=run_params,
	)
