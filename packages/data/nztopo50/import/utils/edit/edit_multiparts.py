"""Delete a feature or replace its multipart geometry with one selected part."""

from __future__ import annotations

import argparse
import re
import sqlite3
import struct
from contextlib import closing
from pathlib import Path

import pyogrio
from shapely import from_wkb, to_wkb
from shapely.geometry import MultiPolygon, Polygon

DEFAULT_GPKG_PATH = Path(
	r"C:\Data\toposource\topographic-data-dev\topographic-data-dev.gpkg"
)


def quote_identifier(identifier: str) -> str:
	"""Return a safely quoted SQLite identifier."""
	return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def load_feature(gpkg_path: Path, layer_name: str, t50_fid: int):
	"""Load exactly one feature by t50_fid, retaining its GeoPackage FID."""
	features = pyogrio.read_dataframe(
		gpkg_path,
		layer=layer_name,
		where=f"t50_fid = {t50_fid}",
		fid_as_index=True,
	)
	if features.empty:
		raise ValueError(f"No feature with t50_fid {t50_fid} in layer {layer_name!r}")
	if len(features) != 1:
		raise ValueError(
			f"Expected one feature with t50_fid {t50_fid}; found {len(features)}"
		)
	return features.iloc[0]


def polygon_parts(geometry: object) -> tuple[Polygon, ...]:
	"""Return the polygons in a Polygon or MultiPolygon geometry."""
	if isinstance(geometry, Polygon):
		return (geometry,)
	if isinstance(geometry, MultiPolygon):
		return tuple(geometry.geoms)
	geometry_type = getattr(geometry, "geom_type", type(geometry).__name__)
	raise ValueError(f"Expected Polygon or MultiPolygon; found {geometry_type}")


def geometry_part_count(geometry: object) -> int:
	"""Return the number of polygon parts, or zero for null/non-polygon geometry."""
	if isinstance(geometry, Polygon):
		return 1
	if isinstance(geometry, MultiPolygon):
		return len(geometry.geoms)
	return 0


def check_all_polygon_layers(gpkg_path: Path) -> int:
	"""Print every feature with more than one polygon part."""
	polygon_layers = [
		str(layer_name)
		for layer_name, geometry_type in pyogrio.list_layers(gpkg_path)
		if geometry_type and str(geometry_type).lower().endswith("polygon")
	]
	total_multipart_features = 0
	for layer_name in polygon_layers:
		fields = set(pyogrio.read_info(gpkg_path, layer=layer_name)["fields"])
		columns = [field for field in ("id", "t50_fid") if field in fields]
		features = pyogrio.read_dataframe(
			gpkg_path,
			layer=layer_name,
			columns=columns,
			read_geometry=True,
			fid_as_index=True,
		)
		part_counts = features.geometry.map(geometry_part_count)
		multipart_features = features.loc[part_counts > 1]
		print(f"{layer_name}: {len(multipart_features)} multipart feature(s)")
		for fid, feature in multipart_features.iterrows():
			feature_id = feature.get("id", "")
			t50_fid = feature.get("t50_fid", "")
			print(
				f"  fid={fid} id={feature_id} t50_fid={t50_fid} "
				f"parts={part_counts.loc[fid]}"
			)
		total_multipart_features += len(multipart_features)

	print(
		f"Checked {len(polygon_layers)} polygon layer(s); "
		f"found {total_multipart_features} multipart feature(s)"
	)
	return total_multipart_features


def print_feature(feature: object, parts: tuple[Polygon, ...]) -> None:
	"""Print identifying fields and the available polygon parts."""
	print(
		f"Feature id={feature['id']} t50_fid={feature.get('t50_fid')} "
		f"type={feature.get('type')} parts={len(parts)}"
	)
	for part_number, part in enumerate(parts, start=1):
		bounds = ", ".join(f"{coordinate:.8f}" for coordinate in part.bounds)
		print(
			f"  {part_number}: area={part.area:.12g}, bounds=({bounds}), "
			f"vertices={len(part.exterior.coords)}"
		)


def encode_gpkg_geometry(geometry: Polygon | MultiPolygon, srs_id: int) -> bytes:
	"""Encode a non-empty geometry as a GeoPackage geometry binary."""
	if geometry.is_empty:
		raise ValueError("Cannot write an empty geometry")
	min_x, min_y, max_x, max_y = geometry.bounds
	header = b"GP" + bytes((0, 3))
	envelope = struct.pack("<4d", min_x, max_x, min_y, max_y)
	return header + struct.pack("<i", srs_id) + envelope + to_wkb(
		geometry, byte_order=1, include_srid=False
	)


def decode_gpkg_geometry(value: bytes):
	"""Decode a GeoPackage geometry binary for SQLite trigger functions."""
	if value[:2] != b"GP" or len(value) < 8:
		raise ValueError("Invalid GeoPackage geometry binary")
	envelope_sizes = (0, 4, 6, 6, 8)
	envelope_type = (value[3] >> 1) & 0b111
	if envelope_type >= len(envelope_sizes):
		raise ValueError(f"Unsupported GeoPackage envelope type {envelope_type}")
	wkb_offset = 8 + envelope_sizes[envelope_type] * 8
	return from_wkb(value[wkb_offset:])


def register_spatial_functions(connection: sqlite3.Connection) -> None:
	"""Register functions used by standard GeoPackage spatial-index triggers."""
	connection.create_function(
		"ST_IsEmpty", 1, lambda value: int(decode_gpkg_geometry(value).is_empty)
	)
	for function_name, bound_index in (
		("ST_MinX", 0),
		("ST_MinY", 1),
		("ST_MaxX", 2),
		("ST_MaxY", 3),
	):
		connection.create_function(
			function_name,
			1,
			lambda value, index=bound_index: decode_gpkg_geometry(value).bounds[index],
		)


def layer_metadata(
	connection: sqlite3.Connection, layer_name: str
) -> tuple[str, str, str, int]:
	"""Return the primary key, geometry column, and SRS ID for a feature layer."""
	primary_keys = [
		row[1]
		for row in connection.execute(
			f"PRAGMA table_info({quote_identifier(layer_name)})"
		)
		if row[5]
	]
	geometry_row = connection.execute(
		"""
		SELECT column_name, geometry_type_name, srs_id
		FROM gpkg_geometry_columns
		WHERE table_name = ?
		""",
		(layer_name,),
	).fetchone()
	if len(primary_keys) != 1 or geometry_row is None:
		raise ValueError(f"Layer {layer_name!r} is not a supported GeoPackage layer")
	geometry_column, geometry_type, srs_id = geometry_row
	if geometry_type.upper() not in {"POLYGON", "MULTIPOLYGON"}:
		raise ValueError(
			f"Layer {layer_name!r} has unsupported geometry type {geometry_type}"
		)
	return primary_keys[0], geometry_column, geometry_type.upper(), srs_id


def rewrite_geometry_column_type(
	connection: sqlite3.Connection, layer_name: str, geometry_column: str
) -> None:
	"""Change the geometry column's declared SQLite type to POLYGON."""
	table_row = connection.execute(
		"SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?",
		(layer_name,),
	).fetchone()
	if table_row is None or table_row[0] is None:
		raise ValueError(f"Could not read the schema for layer {layer_name!r}")

	column_pattern = "|".join(
		re.escape(identifier)
		for identifier in (geometry_column, quote_identifier(geometry_column))
	)
	updated_sql, replacement_count = re.subn(
		rf"(?P<prefix>(?:{column_pattern})\s+)MULTIPOLYGON\b",
		r"\g<prefix>POLYGON",
		table_row[0],
		count=1,
		flags=re.IGNORECASE,
	)
	if replacement_count != 1:
		raise ValueError(
			f"Could not locate the MULTIPOLYGON declaration for "
			f"{layer_name}.{geometry_column}"
		)

	connection.execute("PRAGMA writable_schema = ON")
	try:
		connection.execute(
			"UPDATE sqlite_schema SET sql = ? WHERE type = 'table' AND name = ?",
			(updated_sql, layer_name),
		)
	finally:
		connection.execute("PRAGMA writable_schema = OFF")
	schema_version = connection.execute("PRAGMA schema_version").fetchone()[0]
	connection.execute(f"PRAGMA schema_version = {schema_version + 1}")


def update_geometry_type(gpkg_path: Path, layer_name: str) -> int:
	"""Convert a one-part MULTIPOLYGON layer to POLYGON transactionally."""
	with closing(sqlite3.connect(gpkg_path)) as connection:
		primary_key, geometry_column, geometry_type, srs_id = layer_metadata(
			connection, layer_name
		)
		if geometry_type != "MULTIPOLYGON":
			raise ValueError(
				f"Layer {layer_name!r} is declared {geometry_type}, not MULTIPOLYGON"
			)

		quoted_layer = quote_identifier(layer_name)
		quoted_primary_key = quote_identifier(primary_key)
		quoted_geometry = quote_identifier(geometry_column)
		geometry_rows = connection.execute(
			f"SELECT {quoted_primary_key}, {quoted_geometry} FROM {quoted_layer} "
			f"WHERE {quoted_geometry} IS NOT NULL"
		)
		feature_count = 0
		for fid, geometry_value in geometry_rows:
			geometry = decode_gpkg_geometry(geometry_value)
			if isinstance(geometry, MultiPolygon):
				if len(geometry.geoms) != 1:
					raise ValueError(
						f"Layer {layer_name!r} still has {len(geometry.geoms)} parts "
						f"at fid {fid}; resolve multipart geometries first"
					)
			elif not isinstance(geometry, Polygon):
				raise ValueError(
					f"Layer {layer_name!r} has {geometry.geom_type} at fid {fid}"
				)
			feature_count += 1

		def to_polygon(value: bytes) -> bytes:
			geometry = decode_gpkg_geometry(value)
			polygon = geometry.geoms[0] if isinstance(geometry, MultiPolygon) else geometry
			return encode_gpkg_geometry(polygon, srs_id)

		register_spatial_functions(connection)
		connection.create_function("ToSinglePolygon", 1, to_polygon)
		with connection:
			cursor = connection.execute(
				f"UPDATE {quoted_layer} "
				f"SET {quoted_geometry} = ToSinglePolygon({quoted_geometry}) "
				f"WHERE {quoted_geometry} IS NOT NULL"
			)
			if cursor.rowcount != feature_count:
				raise RuntimeError(
					f"Expected to convert {feature_count} geometries; "
					f"converted {cursor.rowcount}"
				)
			metadata_cursor = connection.execute(
				"""
				UPDATE gpkg_geometry_columns
				SET geometry_type_name = 'POLYGON'
				WHERE table_name = ? AND geometry_type_name = 'MULTIPOLYGON'
				""",
				(layer_name,),
			)
			if metadata_cursor.rowcount != 1:
				raise RuntimeError("GeoPackage geometry metadata changed unexpectedly")
			connection.execute(
				"""
				UPDATE gpkg_contents
				SET last_change = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
				WHERE table_name = ?
				""",
				(layer_name,),
			)
			rewrite_geometry_column_type(connection, layer_name, geometry_column)

	return feature_count


def confirm(prompt: str, assume_yes: bool) -> None:
	"""Require explicit confirmation before changing the GeoPackage."""
	if assume_yes:
		return
	if input(f"{prompt} Type YES to continue: ").strip().casefold() != "yes":
		raise SystemExit("No changes made")


def delete_feature(
	gpkg_path: Path, layer_name: str, t50_fid: int, fid: int
) -> None:
	"""Delete one feature, checking that its FID and t50_fid still match."""
	with closing(sqlite3.connect(gpkg_path)) as connection:
		with connection:
			primary_key, _, _, _ = layer_metadata(connection, layer_name)
			cursor = connection.execute(
				f"DELETE FROM {quote_identifier(layer_name)} "
				f"WHERE {quote_identifier(primary_key)} = ? AND t50_fid = ?",
				(fid, t50_fid),
			)
			if cursor.rowcount != 1:
				raise RuntimeError(
					"Feature changed after it was loaded; transaction rolled back"
				)


def replace_geometry(
	gpkg_path: Path,
	layer_name: str,
	t50_fid: int,
	fid: int,
	selected_part: Polygon,
) -> None:
	"""Replace one feature's geometry with a selected polygon part."""
	with closing(sqlite3.connect(gpkg_path)) as connection:
		register_spatial_functions(connection)
		with connection:
			primary_key, geometry_column, geometry_type, srs_id = layer_metadata(
				connection, layer_name
			)
			geometry = (
				MultiPolygon([selected_part])
				if geometry_type == "MULTIPOLYGON"
				else selected_part
			)
			cursor = connection.execute(
				f"UPDATE {quote_identifier(layer_name)} "
				f"SET {quote_identifier(geometry_column)} = ? "
				f"WHERE {quote_identifier(primary_key)} = ? AND t50_fid = ?",
				(encode_gpkg_geometry(geometry, srs_id), fid, t50_fid),
			)
			if cursor.rowcount != 1:
				raise RuntimeError(
					"Feature changed after it was loaded; transaction rolled back"
				)


def parse_arguments() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description=__doc__)
	parser.add_argument("layer", nargs="?", help="GeoPackage layer, for example land_cover")
	parser.add_argument("t50_fid", nargs="?", type=int, help="Feature t50_fid value")
	parser.add_argument("action", nargs="?", choices=("delete", "select"))
	parser.add_argument(
		"--part",
		type=int,
		help="1-based polygon part to retain; prompted for when omitted",
	)
	parser.add_argument("--gpkg", type=Path, default=DEFAULT_GPKG_PATH)
	parser.add_argument("--yes", action="store_true", help="Skip confirmation")
	parser.add_argument(
		"--check",
		action="store_true",
		help="List features with more than one part in every polygon layer",
	)
	parser.add_argument(
		"--update-geometry-type",
		action="store_true",
		help="Convert one MULTIPOLYGON layer to POLYGON after multipart cleanup",
	)
	return parser.parse_args()


def main() -> None:
	arguments = parse_arguments()
	if not arguments.gpkg.is_file():
		raise FileNotFoundError(f"GeoPackage not found: {arguments.gpkg}")
	if arguments.check:
		if any(
			(
				arguments.layer,
				arguments.t50_fid,
				arguments.action,
				arguments.part,
				arguments.update_geometry_type,
			)
		):
			raise ValueError("--check cannot be combined with feature edit arguments")
		check_all_polygon_layers(arguments.gpkg)
		return
	if arguments.update_geometry_type:
		if not arguments.layer or any(
			(arguments.t50_fid, arguments.action, arguments.part)
		):
			raise ValueError(
				"--update-geometry-type requires one layer and no feature arguments"
			)
		confirm(
			f"Convert every geometry in {arguments.layer} to POLYGON?",
			arguments.yes,
		)
		converted_count = update_geometry_type(arguments.gpkg, arguments.layer)
		print(
			f"Converted {converted_count} geometries in {arguments.layer} to POLYGON"
		)
		return
	if not all((arguments.layer, arguments.t50_fid, arguments.action)):
		raise ValueError(
			"layer, t50_fid, and action are required unless another mode is used"
		)

	feature = load_feature(arguments.gpkg, arguments.layer, arguments.t50_fid)
	parts = polygon_parts(feature.geometry)
	print_feature(feature, parts)
	fid = int(feature.name)

	if arguments.action == "delete":
		confirm(f"Delete feature with t50_fid {arguments.t50_fid}?", arguments.yes)
		delete_feature(arguments.gpkg, arguments.layer, arguments.t50_fid, fid)
		print(f"Deleted t50_fid {arguments.t50_fid} from {arguments.layer}")
		return

	part_number = arguments.part
	if part_number is None:
		part_number = int(input("Part number to retain: "))
	if not 1 <= part_number <= len(parts):
		raise ValueError(f"Part must be between 1 and {len(parts)}")
	confirm(
		f"Replace t50_fid {arguments.t50_fid} with polygon part {part_number}?",
		arguments.yes,
	)
	replace_geometry(
		arguments.gpkg,
		arguments.layer,
		arguments.t50_fid,
		fid,
		parts[part_number - 1],
	)
	print(f"Retained polygon part {part_number} for t50_fid {arguments.t50_fid}")


if __name__ == "__main__":
	main()
