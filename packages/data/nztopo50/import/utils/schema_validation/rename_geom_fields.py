import argparse
import sqlite3
from pathlib import Path


DEFAULT_DATA_PATH = Path(
	r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data\topographic-source-data.gpkg"
)


def quote_identifier(identifier: str) -> str:
	escaped_identifier = identifier.replace('"', '""')
	return f'"{escaped_identifier}"'


def table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
	row = connection.execute(
		"""
		SELECT 1
		FROM sqlite_master
		WHERE type IN ('table', 'view') AND name = ?
		""",
		(table_name,),
	).fetchone()
	return row is not None


def get_geom_layers(connection: sqlite3.Connection) -> list[str]:
	rows = connection.execute(
		"""
		SELECT table_name
		FROM gpkg_geometry_columns
		WHERE column_name = 'geom'
		ORDER BY table_name
		"""
	).fetchall()
	return [row[0] for row in rows]


def get_table_columns(connection: sqlite3.Connection, table_name: str) -> set[str]:
	rows = connection.execute(f"PRAGMA table_info({quote_identifier(table_name)})").fetchall()
	return {row[1] for row in rows}


def rename_geom_column(
	connection: sqlite3.Connection,
	table_name: str,
	*,
	dry_run: bool,
) -> bool:
	columns = get_table_columns(connection, table_name)
	if "geom" not in columns:
		print(f"Skipping {table_name}: metadata says geom, but table column is missing")
		return False
	if "geometry" in columns:
		print(f"Skipping {table_name}: geometry column already exists")
		return False

	print(f"Renaming {table_name}.geom to {table_name}.geometry")

	if dry_run:
		return True

	quoted_table_name = quote_identifier(table_name)
	connection.execute(f"ALTER TABLE {quoted_table_name} RENAME COLUMN geom TO geometry")
	rename_spatial_index_table(connection, table_name)
	connection.execute(
		"""
		UPDATE gpkg_geometry_columns
		SET column_name = 'geometry'
		WHERE table_name = ? AND column_name = 'geom'
		""",
		(table_name,),
	)
	if table_exists(connection, "gpkg_extensions"):
		connection.execute(
			"""
			UPDATE gpkg_extensions
			SET column_name = 'geometry'
			WHERE table_name = ? AND column_name = 'geom'
			""",
			(table_name,),
		)
	return True


def rename_spatial_index_table(connection: sqlite3.Connection, table_name: str) -> None:
	old_rtree_table = f"rtree_{table_name}_geom"
	new_rtree_table = f"rtree_{table_name}_geometry"

	if not table_exists(connection, old_rtree_table):
		return
	if table_exists(connection, new_rtree_table):
		print(f"Skipping spatial index rename for {table_name}: {new_rtree_table} already exists")
		return

	connection.execute(
		f"ALTER TABLE {quote_identifier(old_rtree_table)} RENAME TO {quote_identifier(new_rtree_table)}"
	)


def rename_geom_fields(gpkg_path: Path, *, dry_run: bool, vacuum: bool) -> int:
	if not gpkg_path.exists():
		raise FileNotFoundError(f"GeoPackage not found: {gpkg_path}")

	with sqlite3.connect(gpkg_path) as connection:
		connection.execute("PRAGMA foreign_keys = ON")
		layers = get_geom_layers(connection)

		if not layers:
			print("No GeoPackage layers use a geom geometry column.")
			return 0

		renamed_count = 0
		for layer in layers:
			if rename_geom_column(connection, layer, dry_run=dry_run):
				renamed_count += 1

		if dry_run:
			print(f"Dry run complete: {renamed_count} layer(s) would be renamed.")
			return renamed_count

		connection.execute("REINDEX")
		connection.commit()

	if vacuum and renamed_count:
		with sqlite3.connect(gpkg_path) as connection:
			connection.execute("VACUUM")

	print(f"Complete: renamed {renamed_count} layer(s).")
	return renamed_count


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Rename GeoPackage geometry columns from geom to geometry."
	)
	parser.add_argument(
		"gpkg_path",
		nargs="?",
		type=Path,
		default=DEFAULT_DATA_PATH,
		help=f"Path to the GeoPackage. Defaults to {DEFAULT_DATA_PATH}",
	)
	parser.add_argument(
		"--dry-run",
		action="store_true",
		help="Print the layers that would be changed without modifying the GeoPackage.",
	)
	parser.add_argument(
		"--vacuum",
		action="store_true",
		help="Run VACUUM after renaming columns. This rewrites the GeoPackage file.",
	)
	return parser.parse_args()


if __name__ == "__main__":
	args = parse_args()
	rename_geom_fields(args.gpkg_path, dry_run=args.dry_run, vacuum=args.vacuum)