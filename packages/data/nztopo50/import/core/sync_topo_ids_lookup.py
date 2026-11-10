#!/usr/bin/env python3
"""Update release tables from lookups.topoid_lkp using t50_fid + layer_name.

For each table in a target schema, this script:
- matches rows on `t50_fid`
- filters lookup rows by `layer_name = table_name`
- updates identifier column (`id` or `topo_id`) from lookup `topo_id`
- updates date columns (`created_at`/`create_date`, `updated_at`/`update_date`)
  from lookup `created_at`
"""

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


class TopoIdLookupUpdater:
	"""Apply lookup-based id/date updates across a schema."""

	def __init__(self, db_params: dict[str, Any]):
		self.db_params = db_params
		self.conn: psycopg.Connection | None = None
		self.connect()

	def connect(self) -> None:
		if self.conn is None or self.conn.closed:
			self.conn = psycopg.connect(**self.db_params)

	def close(self) -> None:
		if self.conn and not self.conn.closed:
			self.conn.close()

	def list_schema_tables(self, schema_name: str) -> list[str]:
		self.connect()
		query = """
			SELECT table_name
			FROM information_schema.tables
			WHERE table_schema = %s AND table_type = 'BASE TABLE'
			ORDER BY table_name
		"""
		with self.conn.cursor() as cur:
			cur.execute(query, (schema_name,))
			return [row[0] for row in cur.fetchall()]

	def table_exists(self, schema: str, table: str) -> bool:
		self.connect()
		query = """
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = %s AND table_name = %s
		"""
		with self.conn.cursor() as cur:
			cur.execute(query, (schema, table))
			return cur.fetchone() is not None

	def column_exists(self, schema: str, table: str, column_name: str) -> bool:
		self.connect()
		query = """
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = %s AND table_name = %s AND column_name = %s
		"""
		with self.conn.cursor() as cur:
			cur.execute(query, (schema, table, column_name))
			return cur.fetchone() is not None

	def _first_existing_column(
		self, schema: str, table: str, candidates: list[str]
	) -> str | None:
		for candidate in candidates:
			if self.column_exists(schema, table, candidate):
				return candidate
		return None

	def update_table_from_lookup(
		self,
		target_schema: str,
		table_name: str,
		lookup_schema: str = "lookups",
		lookup_table: str = "topoid_lkp",
	) -> bool:
		"""Update one table from lookups.topoid_lkp.

		Returns:
			True if table processed successfully (including zero-row updates),
			otherwise False when prerequisites are missing or SQL fails.
		"""
		self.connect()

		if not self.table_exists(target_schema, table_name):
			print(f"Skipping {target_schema}.{table_name}: table not found")
			return False

		if not self.table_exists(lookup_schema, lookup_table):
			print(f"Lookup table missing: {lookup_schema}.{lookup_table}")
			return False

		if not self.column_exists(target_schema, table_name, "t50_fid"):
			print(f"Skipping {target_schema}.{table_name}: missing t50_fid")
			return False

		id_column = self._first_existing_column(target_schema, table_name, ["id", "topo_id"])
		created_column = self._first_existing_column(
			target_schema, table_name, ["created_at", "create_date"]
		)
		updated_column = self._first_existing_column(
			target_schema, table_name, ["updated_at", "update_date"]
		)

		missing_targets = []
		if id_column is None:
			missing_targets.append("id/topo_id")
		if created_column is None:
			missing_targets.append("created_at/create_date")
		if updated_column is None:
			missing_targets.append("updated_at/update_date")
		if missing_targets:
			print(
				f"Skipping {target_schema}.{table_name}: missing {', '.join(missing_targets)}"
			)
			return False

		# Deduplicate lookup rows by t50_fid per layer, preferring latest created_at.
		count_query = sql.SQL(
			"""
			WITH src AS (
				SELECT DISTINCT ON (t50_fid)
					t50_fid,
					topo_id::uuid,
					created_at
				FROM {}.{}
				WHERE layer_name = %s
				  AND topo_id IS NOT NULL
				  AND created_at IS NOT NULL
				ORDER BY t50_fid, created_at DESC
			)
			SELECT COUNT(*)
			FROM {}.{} AS tgt
			JOIN src ON tgt.t50_fid = src.t50_fid
			WHERE tgt.{} IS DISTINCT FROM src.topo_id
			   OR tgt.{} IS DISTINCT FROM src.created_at
			   OR tgt.{} IS DISTINCT FROM src.created_at
			"""
		).format(
			sql.Identifier(lookup_schema),
			sql.Identifier(lookup_table),
			sql.Identifier(target_schema),
			sql.Identifier(table_name),
			sql.Identifier(id_column),
			sql.Identifier(created_column),
			sql.Identifier(updated_column),
		)

		update_query = sql.SQL(
			"""
			WITH src AS (
				SELECT DISTINCT ON (t50_fid)
					t50_fid,
					topo_id::uuid,
					created_at
				FROM {}.{}
				WHERE layer_name = %s
				  AND topo_id IS NOT NULL
				  AND created_at IS NOT NULL
				ORDER BY t50_fid, created_at DESC
			)
			UPDATE {}.{} AS tgt
			SET
				{} = src.topo_id,
				{} = src.created_at,
				{} = src.created_at
			FROM src
			WHERE tgt.t50_fid = src.t50_fid
			"""
		).format(
			sql.Identifier(lookup_schema),
			sql.Identifier(lookup_table),
			sql.Identifier(target_schema),
			sql.Identifier(table_name),
			sql.Identifier(id_column),
			sql.Identifier(created_column),
			sql.Identifier(updated_column),
			sql.Identifier(id_column),
			sql.Identifier(created_column),
			sql.Identifier(updated_column),
		)

		try:
			with self.conn.cursor() as cur:
				cur.execute(count_query, (table_name,))
				to_update = cur.fetchone()[0]

				if to_update == 0:
					print(f"No updates needed for {target_schema}.{table_name}")
					return True

				cur.execute(update_query, (table_name,))
				self.conn.commit()
				print(
					f"Updated {to_update} rows in {target_schema}.{table_name} "
					f"using {lookup_schema}.{lookup_table}"
				)
				return True
		except Exception as exc:
			self.conn.rollback()
			print(f"Error updating {target_schema}.{table_name}: {exc}")
			return False


def update_schema_from_lookup(
	target_schema: str,
	lookup_schema: str = "lookups",
	lookup_table: str = "topoid_lkp",
	skip_tables: list[str] | None = None,
) -> tuple[int, int]:
	"""Update all tables in target schema from lookup table.

	Returns:
		Tuple of (successful_count, failed_or_skipped_count).
	"""
	updater = TopoIdLookupUpdater(DB_PARAMS)
	skip_tables = skip_tables or []

	tables = updater.list_schema_tables(target_schema)

	success_count = 0
	failed_count = 0

	print(
		f"Starting lookup updates for schema '{target_schema}' from "
		f"{lookup_schema}.{lookup_table}"
	)

	for table_name in tables:
		if table_name in skip_tables:
			print(f"Skipping table: {target_schema}.{table_name}")
			continue

		ok = updater.update_table_from_lookup(
			target_schema=target_schema,
			table_name=table_name,
			lookup_schema=lookup_schema,
			lookup_table=lookup_table,
		)
		if ok:
			success_count += 1
		else:
			failed_count += 1

	print("\n=== LOOKUP UPDATE SUMMARY ===")
	print(f"Tables processed: {len(tables) - len(skip_tables)}")
	print(f"Successful: {success_count}")
	print(f"Failed/Skipped: {failed_count}")

	updater.close()
	return success_count, failed_count


def main() -> None:
	target_schema = "release66"
	lookup_schema = "lookups"
	lookup_table = "topoid_lkp"

	# Optional exclusions
	skip_tables: list[str] = []

	update_schema_from_lookup(
		target_schema=target_schema,
		lookup_schema=lookup_schema,
		lookup_table=lookup_table,
		skip_tables=skip_tables,
	)


if __name__ == "__main__":
	main()
