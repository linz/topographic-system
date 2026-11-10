from __future__ import annotations

from typing import Any

import psycopg
from psycopg import sql


DB_PARAMS: dict[str, Any] = {
	"dbname": "topo",
	"user": "postgres",
	"password": "landinformation",
	"host": "localhost",
	"port": 5432,
}


def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
	with conn.cursor() as cur:
		cur.execute(sql.SQL('CREATE SCHEMA IF NOT EXISTS {};').format(sql.Identifier(schema)))
	conn.commit()


def table_exists(conn: psycopg.Connection, schema: str, table: str) -> bool:
	query = """
		SELECT 1
		FROM information_schema.tables
		WHERE table_schema = %s AND table_name = %s
	"""
	with conn.cursor() as cur:
		cur.execute(query, (schema, table.lower()))
		return cur.fetchone() is not None


def drop_table_if_exists(conn: psycopg.Connection, schema: str, table: str) -> None:
	with conn.cursor() as cur:
		cur.execute(
			sql.SQL("DROP TABLE IF EXISTS {}.{};").format(
				sql.Identifier(schema),
				sql.Identifier(table.lower()),
			)
		)
	conn.commit()


def create_lookup_table(conn: psycopg.Connection, schema: str, table: str) -> None:
	with conn.cursor() as cur:
		cur.execute(
			sql.SQL(
				"""
				CREATE TABLE IF NOT EXISTS {}.{} (
					topo_id text,
					t50_fid bigint,
					type text,
					created_at timestamp,
					layer_name text
				);
				"""
			).format(
				sql.Identifier(schema),
				sql.Identifier(table.lower()),
			)
		)
	conn.commit()


def create_indexes(conn: psycopg.Connection, schema: str, table: str) -> None:
	table_name = table.lower()
	with conn.cursor() as cur:
		cur.execute(
			sql.SQL('CREATE INDEX IF NOT EXISTS {} ON {}.{} (topo_id);').format(
				sql.Identifier(f"{table_name}_topo_id_idx"),
				sql.Identifier(schema),
				sql.Identifier(table_name),
			)
		)
		cur.execute(
			sql.SQL('CREATE INDEX IF NOT EXISTS {} ON {}.{} (t50_fid);').format(
				sql.Identifier(f"{table_name}_t50_fid_idx"),
				sql.Identifier(schema),
				sql.Identifier(table_name),
			)
		)
		cur.execute(
			sql.SQL('CREATE INDEX IF NOT EXISTS {} ON {}.{} (layer_name);').format(
				sql.Identifier(f"{table_name}_layer_name_idx"),
				sql.Identifier(schema),
				sql.Identifier(table_name),
			)
		)
	conn.commit()


def list_base_tables(conn: psycopg.Connection, schema: str) -> list[str]:
	query = """
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = %s AND table_type = 'BASE TABLE'
		ORDER BY table_name
	"""
	with conn.cursor() as cur:
		cur.execute(query, (schema,))
		return [row[0] for row in cur.fetchall()]


def list_columns(conn: psycopg.Connection, schema: str, table: str) -> list[str]:
	query = """
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = %s AND table_name = %s
	"""
	with conn.cursor() as cur:
		cur.execute(query, (schema, table))
		return [row[0] for row in cur.fetchall()]


def first_existing_column(columns: list[str], candidates: list[str]) -> str | None:
	available = {name.lower(): name for name in columns}
	for candidate in candidates:
		hit = available.get(candidate.lower())
		if hit is not None:
			return hit
	return None


def copy_topoid_lookup_from_schema(
	source_schema: str,
	target_schema: str,
	target_table: str,
	if_exists: str,
	params: dict[str, Any],
) -> None:
	with psycopg.connect(**params) as conn:
		ensure_schema(conn, target_schema)

		target_exists = table_exists(conn, target_schema, target_table)
		if if_exists == "fail" and target_exists:
			raise ValueError(f"Target table already exists: {target_schema}.{target_table}")
		if if_exists == "replace":
			drop_table_if_exists(conn, target_schema, target_table)

		create_lookup_table(conn, target_schema, target_table)

		tables = list_base_tables(conn, source_schema)
		if not tables:
			raise ValueError(f"No tables found in source schema: {source_schema}")

		total_inserted = 0
		for table_name in tables:
			columns = list_columns(conn, source_schema, table_name)

			topo_col = first_existing_column(columns, ["topo_id", "id"])
			t50_fid_col = first_existing_column(columns, ["t50_fid"])
			type_col = first_existing_column(columns, ["type"])
			created_col = first_existing_column(columns, ["created_at", "create_date"])

			if topo_col is None or t50_fid_col is None:
				print(
					f"Skipping {source_schema}.{table_name} - missing required fields: topo_id/id or t50_fid"
				)
				continue

			type_expr = (
				sql.SQL("NULL")
				if type_col is None
				else sql.SQL("{}.{}").format(sql.Identifier("src"), sql.Identifier(type_col))
			)
			created_expr = (
				sql.SQL("NULL")
				if created_col is None
				else sql.SQL("{}.{}").format(
					sql.Identifier("src"), sql.Identifier(created_col)
				)
			)

			insert_query = sql.SQL(
				"""
				INSERT INTO {}.{} (topo_id, t50_fid, type, created_at, layer_name)
				SELECT
					src.{}::text,
					src.{}::bigint,
					{}::text,
					{}::timestamp,
					%s
				FROM {}.{} AS src
				WHERE src.{} IS NOT NULL
				  AND src.{} IS NOT NULL;
				"""
			).format(
				sql.Identifier(target_schema),
				sql.Identifier(target_table.lower()),
				sql.Identifier(topo_col),
				sql.Identifier(t50_fid_col),
				type_expr,
				created_expr,
				sql.Identifier(source_schema),
				sql.Identifier(table_name),
				sql.Identifier(topo_col),
				sql.Identifier(t50_fid_col),
			)

			try:
				with conn.cursor() as cur:
					cur.execute(insert_query, (table_name,))
					inserted = cur.rowcount or 0
				conn.commit()
				total_inserted += inserted
				print(
					f"Copied {inserted} rows from {source_schema}.{table_name} to {target_schema}.{target_table.lower()}"
				)
			except Exception as exc:
				conn.rollback()
				print(f"Error copying from {source_schema}.{table_name}: {exc}")

		create_indexes(conn, target_schema, target_table)

		print(
			f"Loaded {total_inserted} rows from schema '{source_schema}' into {target_schema}.{target_table.lower()}"
		)


if __name__ == "__main__":
	source_schema = "release66"
	target_schema = "lookups"
	target_table = "topoid_lkp"
	if_exists = "replace"  # fail | replace | append

	copy_topoid_lookup_from_schema(
		source_schema=source_schema,
		target_schema=target_schema,
		target_table=target_table,
		if_exists=if_exists,
		params=DB_PARAMS,
	)
