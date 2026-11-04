

import argparse
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

import geopandas as gpd
import pandas as pd
import pyogrio

OUTPUT_DIRECTORY = Path(r"C:\Temp\export")


@dataclass(frozen=True)
class LayerPair:
	lds_db: str
	lds_layer: str
	lamps_db: str
	lamps_layer: str


LAYER_PAIRS = [
	LayerPair(
		lds_db=r"C:\Data\Topo50\kart-source\Latest\nz-canal-centrelines-topo-150k\nz-canal-centrelines-topo-150k.gpkg",
		lds_layer="nz_canal_centrelines_topo_150k",
		lamps_db=r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data\topographic-source-data.gpkg",
		lamps_layer="canal_cl",
	),
	LayerPair(
			lds_db=r"C:\Data\Topo50\kart-source\Latest\nz-canal-polygons-topo-150k\nz-canal-polygons-topo-150k.gpkg",
			lds_layer="nz_canal_polygons_topo_150k",
			lamps_db=r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data\topographic-source-data.gpkg",
			lamps_layer="canal_poly",
		),
	LayerPair(
			lds_db=r"C:\Data\Topo50\kart-source\Latest\nz-drain-centrelines-topo-150k\nz-drain-centrelines-topo-150k.gpkg",
			lds_layer="nz_drain_centrelines_topo_150k",
			lamps_db=r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data\topographic-source-data.gpkg",
			lamps_layer="drain_cl",
		),
		LayerPair(
			lds_db=r"C:\Data\Topo50\kart-source\Latest\nz-river-centrelines-topo-150k\nz-river-centrelines-topo-150k.gpkg",
			lds_layer="nz_river_centrelines_topo_150k",
			lamps_db=r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data\topographic-source-data.gpkg",
			lamps_layer="river_cl",
		),
		LayerPair(
			lds_db=r"C:\Data\Topo50\kart-source\Latest\nz-river-polygons-topo-150k\nz-river-polygons-topo-150k.gpkg",
			lds_layer="nz_river_polygons_topo_150k",
			lamps_db=r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data\topographic-source-data.gpkg",
			lamps_layer="river_poly",
		),

		LayerPair(
			lds_db=r"C:\Data\Topo50\kart-source\Latest\nz-lagoon-polygons-topo-150k\nz-lagoon-polygons-topo-150k.gpkg",
			lds_layer="nz_lagoon_polygons_topo_150k",
			lamps_db=r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data\topographic-source-data.gpkg",
			lamps_layer="lagoon_poly",
		),
		LayerPair(
			lds_db=r"C:\Data\Topo50\kart-source\Latest\nz-lake-polygons-topo-150k-2\nz-lake-polygons-topo-150k-2.gpkg",
			lds_layer="nz_lake_polygons_topo_150k",
			lamps_db=r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data\topographic-source-data.gpkg",
			lamps_layer="lake_poly",
		),
		LayerPair(
			lds_db=r"C:\Data\Topo50\kart-source\Latest\nz-road-centrelines-topo-150k\nz-road-centrelines-topo-150k.gpkg",
			lds_layer="nz_road_centrelines_topo_150k",
			lamps_db=r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data\topographic-source-data.gpkg",
			lamps_layer="linz_road_cl",
		),
]

LAYER_PAIRS = [
	LayerPair(
		lds_db=r"C:\Data\Topo50\kart-source\Latest\nz-canal-centrelines-topo-150k\nz-canal-centrelines-topo-150k.gpkg",
		lds_layer="nz_canal_centrelines_topo_150k",
		lamps_db=r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data\topographic-source-data.gpkg",
		lamps_layer="canal_cl",
	),
	LayerPair(
			lds_db=r"C:\Data\Topo50\kart-source\Latest\nz-canal-polygons-topo-150k\nz-canal-polygons-topo-150k.gpkg",
			lds_layer="nz_canal_polygons_topo_150k",
			lamps_db=r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data\topographic-source-data.gpkg",
			lamps_layer="canal_poly",
		),
	LayerPair(
			lds_db=r"C:\Data\Topo50\kart-source\Latest\nz-drain-centrelines-topo-150k\nz-drain-centrelines-topo-150k.gpkg",
			lds_layer="nz_drain_centrelines_topo_150k",
			lamps_db=r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data\topographic-source-data.gpkg",
			lamps_layer="drain_cl",
		),
		LayerPair(
			lds_db=r"C:\Data\Topo50\kart-source\Latest\nz-river-centrelines-topo-150k\nz-river-centrelines-topo-150k.gpkg",
			lds_layer="nz_river_centrelines_topo_150k",
			lamps_db=r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data\topographic-source-data.gpkg",
			lamps_layer="river_cl",
		),
		LayerPair(
			lds_db=r"C:\Data\Topo50\kart-source\Latest\nz-river-polygons-topo-150k\nz-river-polygons-topo-150k.gpkg",
			lds_layer="nz_river_polygons_topo_150k",
			lamps_db=r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data\topographic-source-data.gpkg",
			lamps_layer="river_poly",
		),

		LayerPair(
			lds_db=r"C:\Data\Topo50\kart-source\Latest\nz-lagoon-polygons-topo-150k\nz-lagoon-polygons-topo-150k.gpkg",
			lds_layer="nz_lagoon_polygons_topo_150k",
			lamps_db=r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data\topographic-source-data.gpkg",
			lamps_layer="lagoon_poly",
		),
		LayerPair(
			lds_db=r"C:\Data\Topo50\kart-source\Latest\nz-lake-polygons-topo-150k-2\nz-lake-polygons-topo-150k-2.gpkg",
			lds_layer="nz_lake_polygons_topo_150k",
			lamps_db=r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data\topographic-source-data.gpkg",
			lamps_layer="lake_poly",
		),
		LayerPair(
			lds_db=r"C:\Data\Topo50\kart-source\Latest\nz-road-centrelines-topo-150k\nz-road-centrelines-topo-150k.gpkg",
			lds_layer="nz_road_centrelines_topo_150k",
			lamps_db=r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data\topographic-source-data.gpkg",
			lamps_layer="linz_road_cl",
		),
]


def read_layer(path: str, layer_name: str) -> gpd.GeoDataFrame:
	"""Read a geospatial layer and require the matching identifier."""
	layer = gpd.read_file(path, layer=layer_name, fid_as_index=True)
	if layer.index.name != "fid":
		raise ValueError(f"Layer {layer_name!r} does not expose its feature ID")
	layer["_source_fid"] = layer.index
	if "t50_fid" not in layer.columns and layer.index.name == "fid":
		layer["t50_fid"] = layer.index
	print(f"{layer_name} header after read: {list(layer.columns)}")
	if "t50_fid" not in layer.columns:
		raise ValueError(f"Layer {layer_name!r} does not contain t50_fid")
	layer["t50_fid"] = (
		pd.to_numeric(layer["t50_fid"], errors="raise")
		.astype("Int64")
	)
	return layer


def match_layer_pair(pair: LayerPair) -> tuple[
	gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame
]:
	"""Return matched, LDS-only, and LAMPS-only features by t50_fid."""
	lds = read_layer(pair.lds_db, pair.lds_layer)
	lamps = read_layer(pair.lamps_db, pair.lamps_layer)
	if lds.crs != lamps.crs:
		if lds.crs.to_epsg() == lamps.crs.to_epsg():
			lamps = lamps.set_crs(lds.crs, allow_override=True)
		else:
			lamps = lamps.to_crs(lds.crs)

	if lds["t50_fid"].duplicated().any():
		raise ValueError(f"LDS layer {pair.lds_layer!r} has duplicate t50_fid values")
	duplicate_lamps_mask = lamps["t50_fid"].duplicated()
	if duplicate_lamps_mask.any():
		next_lamps_fid = int(lamps["t50_fid"].max()) + 1
		duplicate_lamps_indices = lamps.index[duplicate_lamps_mask]
		new_lamps_fids = range(
			next_lamps_fid,
			next_lamps_fid + len(duplicate_lamps_indices),
		)
		lamps.loc[duplicate_lamps_indices, "t50_fid"] = list(new_lamps_fids)
		print(
			f"Renumbered {len(duplicate_lamps_indices)} duplicate LAMPS "
			f"t50_fid values in {pair.lamps_layer!r}, starting at "
			f"{next_lamps_fid}"
		)

	lds = lds.rename_geometry("lds_geometry").rename(
		columns={
			"t50_fid": "lds_t50_fid",
			"_source_fid": "lds_source_fid",
		}
	)
	lamps = lamps.rename_geometry("lamps_geometry").rename(
		columns={
			"t50_fid": "lamps_t50_fid",
			"_source_fid": "lamps_source_fid",
		}
	)
	print(f"{pair.lds_layer} header before join: {list(lds.columns)}")
	print(f"{pair.lamps_layer} header before join: {list(lamps.columns)}")
	joined = lds.merge(
		lamps,
		left_on="lds_t50_fid",
		right_on="lamps_t50_fid",
		how="outer",
		indicator=True,
		suffixes=("_lds", "_lamps"),
	)
	print(f"{pair.lds_layer}/{pair.lamps_layer} header after join: {list(joined.columns)}")
	joined["t50_fid"] = joined["lds_t50_fid"].combine_first(
		joined["lamps_t50_fid"]
	)

	matched = joined.loc[joined["_merge"] == "both"].copy()
	lds_only = joined.loc[joined["_merge"] == "left_only"].copy()
	lamps_only = joined.loc[joined["_merge"] == "right_only"].copy()
	if "name_lds" in lds_only.columns:
		lds_only = lds_only.loc[lds_only["name_lds"].notna()].copy()
	else:
		lamps_name = first_available_column(lamps_only, ["name_lamps", "name"])
		lamps_only = lamps_only.loc[lamps_name.notna()].copy()
	print(
		"First 10 LDS-only t50_fid values: "
		f"{lds_only['lds_t50_fid'].head(10).tolist()}"
	)
	return matched, lds_only, lamps_only


def process_pairs(pairs: list[LayerPair], output_directory: Path) -> None:
	output_directory.mkdir(parents=True, exist_ok=True)
	for pair in pairs:
		started_at = perf_counter()
		matched, lds_only, lamps_only = match_layer_pair(pair)
		output_path = output_directory / f"{pair.lamps_layer}_matched_fid.parquet"
		write_matched_geoparquet(matched, output_path)
		geometry_matches = match_unmatched_geometry(lds_only, lamps_only)
		geometry_output_path = (
			output_directory / f"{pair.lamps_layer}_matched_spatial.parquet"
		)
		geometry_matches.to_parquet(geometry_output_path, index=False)
		print(
			f"{pair.lds_layer} / {pair.lamps_layer}: "
			f"matched={len(matched)}, "
			f"lds_only={len(lds_only)}, "
			f"lamps_only={len(lamps_only)}, "
			f"output={output_path}, "
			f"geometry_matches={len(geometry_matches)}, "
			f"geometry_output={geometry_output_path}, "
			f"elapsed_seconds={perf_counter() - started_at:.2f}"
		)


def first_available_column(
	layer: pd.DataFrame, column_names: list[str]
) -> pd.Series:
	available = [name for name in column_names if name in layer.columns]
	if not available:
		return pd.Series(pd.NA, index=layer.index, dtype="object")

	values = layer[available[0]]
	for column_name in available[1:]:
		values = values.combine_first(layer[column_name])
	return values


def write_matched_geoparquet(
	matched: gpd.GeoDataFrame, output_path: Path
) -> None:
	"""Write the requested fields from matched LDS/LAMPS features."""
	output = pd.DataFrame(
		{
			"lds_fid": matched["lds_t50_fid"],
			"lamps_fid": matched["lamps_t50_fid"],
			"auto_pk": matched["lamps_source_fid"].astype("Int64"),
			"name": first_available_column(
				matched, ["name_lds", "name_lamps", "name"]
			),
			"gazfeatid": first_available_column(
				matched, ["gazfeatid_lds", "gazfeatid_lamps", "gazfeatid"]
			),
			"geometry": first_available_column(
				matched, ["lds_geometry", "lamps_geometry", "geometry"]
			),
		},
		index=matched.index,
	)
	output = gpd.GeoDataFrame(output, geometry="geometry", crs=matched.crs)
	output_path.parent.mkdir(parents=True, exist_ok=True)
	output.to_parquet(output_path, index=False)


def row_value(row: pd.Series, column_names: list[str]):
	for column_name in column_names:
		if column_name in row.index and pd.notna(row[column_name]):
			return row[column_name]
	return pd.NA


def match_unmatched_geometry(
	lds_only: gpd.GeoDataFrame, lamps_only: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
	"""Match unmatched LAMPS features using geometry-type-specific spatial rules."""
	output_columns = [
		"lds_t50_fid",
		"lamps_t50_fid",
		"lamps_source_fid",
		"auto_pk",
		"name",
		"gazfeatid",
		"geometry",
	]
	if lds_only.empty or lamps_only.empty:
		return gpd.GeoDataFrame(
			columns=output_columns,
			geometry="geometry",
			crs=lds_only.crs,
		)

	lamps_geometry = gpd.GeoSeries(
		lamps_only["lamps_geometry"],
		index=lamps_only.index,
		crs=lamps_only.crs,
	)
	lamps_spatial_index = lamps_geometry.sindex
	used_lamps_indices = set()
	records = []

	for _, lds_row in lds_only.iterrows():
		lds_geometry = lds_row["lds_geometry"]
		if lds_geometry is None or pd.isna(lds_geometry):
			continue

		if lds_geometry.geom_type in {"LineString", "MultiLineString"}:
			query_geometry = lds_geometry.interpolate(0.5, normalized=True).buffer(1)
			predicate = "intersects"
		elif lds_geometry.geom_type in {"Polygon", "MultiPolygon"}:
			query_geometry = lds_geometry.representative_point()
			predicate = "within"
		else:
			query_geometry = lds_geometry.buffer(1)
			predicate = "intersects"

		candidate_positions = lamps_spatial_index.query(
			query_geometry,
			predicate=predicate,
		)
		for candidate_position in candidate_positions:
			lamps_index = lamps_geometry.index[candidate_position]
			if lamps_index in used_lamps_indices:
				continue
			lamps_geometry_value = lamps_geometry.loc[lamps_index]
			if lamps_geometry_value is None or pd.isna(lamps_geometry_value):
				continue

			lamps_row = lamps_only.loc[lamps_index]
			records.append(
				{
					"lds_t50_fid": lds_row["lds_t50_fid"],
					"lamps_t50_fid": lamps_row["lamps_t50_fid"],
					"lamps_source_fid": lamps_row["lamps_source_fid"],
					"auto_pk": lamps_row["lamps_source_fid"],
					"name": row_value(
						lamps_row, ["name_lamps", "name"]
					),
					"gazfeatid": row_value(
						lamps_row, ["gazfeatid_lamps", "gazfeatid"]
					),
					"geometry": lds_geometry,
				}
			)
			used_lamps_indices.add(lamps_index)
			break

	output = gpd.GeoDataFrame(
		records,
		columns=output_columns,
		geometry="geometry",
		crs=lds_only.crs,
	)
	output["auto_pk"] = pd.to_numeric(output["auto_pk"], errors="raise").astype(
		"Int64"
	)
	return output


def quote_identifier(identifier: str) -> str:
	return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def register_spatial_trigger_functions(connection: sqlite3.Connection) -> None:
	"""Support GeoPackage RTree triggers during non-geometry SQLite updates."""
	connection.create_function(
		"ST_IsEmpty",
		1,
		lambda geometry: 1 if geometry is None else 0,
		deterministic=True,
	)


def load_spatial_updates(
	pair: LayerPair, output_directory: Path
) -> tuple[str, list[tuple[int, int]]]:
	output_path = output_directory / f"{pair.lamps_layer}_matched_spatial.parquet"
	if not output_path.exists():
		raise FileNotFoundError(f"Spatial match output not found: {output_path}")

	mapping = pd.read_parquet(
		output_path,
		columns=["lds_t50_fid", "auto_pk"],
	)
	if mapping.isna().any().any():
		raise ValueError(f"Null auto_pk or LDS identifier in {output_path}")
	if mapping["auto_pk"].duplicated().any():
		raise ValueError(f"Duplicate auto_pk values in {output_path}")

	updates = [
		(int(row.lds_t50_fid), int(row.auto_pk))
		for row in mapping.itertuples(index=False)
	]
	info = pyogrio.read_info(pair.lamps_db, layer=pair.lamps_layer)
	key_column = str(info["fid_column"])
	if key_column not in {"fid", "auto_pk"}:
		raise ValueError(
			f"Layer {pair.lamps_layer!r} uses unsupported feature ID "
			f"column {key_column!r}; expected 'fid' or 'auto_pk'"
		)
	if "t50_fid" not in info["fields"]:
		raise ValueError(f"Layer {pair.lamps_layer!r} has no t50_fid column")
	return key_column, updates


def validate_source_keys(
	connection: sqlite3.Connection,
	layer_name: str,
	key_column: str,
	updates: list[tuple[int, int]],
) -> None:
	if not updates:
		return

	source_keys = [source_key for _, source_key in updates]
	matched_source_keys: set[int] = set()
	quoted_key_column = quote_identifier(key_column)
	for start in range(0, len(source_keys), 900):
		chunk = source_keys[start : start + 900]
		placeholders = ", ".join("?" for _ in chunk)
		rows = connection.execute(
			f"SELECT {quoted_key_column} "
			f"FROM {quote_identifier(layer_name)} "
			f"WHERE {quoted_key_column} IN ({placeholders})",
			chunk,
		).fetchall()
		matched_source_keys.update(int(row[0]) for row in rows)

	missing = set(source_keys) - matched_source_keys
	if missing:
		raise ValueError(
			f"Layer {layer_name!r} is missing {len(missing)} "
			f"{key_column} value(s)"
		)


def update_source_fids(
	pairs: list[LayerPair], output_directory: Path, *, dry_run: bool
) -> None:
	pairs_by_database: dict[Path, list[LayerPair]] = {}
	for pair in pairs:
		pairs_by_database.setdefault(Path(pair.lamps_db), []).append(pair)

	for database_path, database_pairs in pairs_by_database.items():
		plans: list[tuple[LayerPair, str, list[tuple[int, int]]]] = []
		with sqlite3.connect(database_path) as connection:
			register_spatial_trigger_functions(connection)
			for pair in database_pairs:
				key_column, updates = load_spatial_updates(pair, output_directory)
				validate_source_keys(
					connection, pair.lamps_layer, key_column, updates
				)
				plans.append((pair, key_column, updates))

			for pair, key_column, updates in plans:
				if not dry_run and updates:
					cursor = connection.executemany(
						f"UPDATE {quote_identifier(pair.lamps_layer)} "
						"SET t50_fid = ? "
						f"WHERE {quote_identifier(key_column)} = ?",
						updates,
					)
					if cursor.rowcount != len(updates):
						raise RuntimeError(
							f"Updated {cursor.rowcount} of {len(updates)} rows in "
							f"{pair.lamps_layer!r}"
						)
				print(
					f"{'Would update' if dry_run else 'Updated'} {len(updates)} "
					f"t50_fid value(s) in {pair.lamps_layer!r} using "
					f"{key_column}"
				)

	print("Source update dry run complete." if dry_run else "Source update complete.")


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Match LDS and LAMPS features and optionally update source t50_fid values."
	)
	parser.add_argument(
		"--skip-matching",
		action="store_true",
		help="Use existing spatial-match Parquet files instead of regenerating them.",
	)
	parser.add_argument(
		"--update-source-fids",
		action="store_true",
		help="Update LAMPS t50_fid values from the spatial-match outputs.",
	)
	parser.add_argument(
		"--dry-run-source-update",
		action="store_true",
		help="Validate and report source updates without changing the GeoPackage.",
	)
	args = parser.parse_args()
	if args.skip_matching and not args.update_source_fids:
		parser.error("--skip-matching requires --update-source-fids")
	if args.dry_run_source_update and not args.update_source_fids:
		parser.error("--dry-run-source-update requires --update-source-fids")
	return args


if __name__ == "__main__":
	args = parse_args()
	if not args.skip_matching:
		process_pairs(LAYER_PAIRS, OUTPUT_DIRECTORY)
	if args.update_source_fids:
		update_source_fids(
			LAYER_PAIRS,
			OUTPUT_DIRECTORY,
			dry_run=args.dry_run_source_update,
		)

