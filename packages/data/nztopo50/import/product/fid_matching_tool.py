

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

import geopandas as gpd
import pandas as pd

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
			lds_db=r"C:\Data\Topo50\kart-source\Latest\nz-road-centrelines-topo-150k\nz-road-centrelines-topo-150k.gpkg",
			lds_layer="nz_road_centrelines_topo_150k",
			lamps_db=r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data\topographic-source-data.gpkg",
			lamps_layer="linz_road_cl",
		),
		LayerPair(
			lds_db=r"C:\Data\Topo50\kart-source\Latest\nz-lagoon_polygons-topo-150k\nz-lagoon_polygons-topo-150k.gpkg",
			lds_layer="nz_lagoon_polygons_topo_150k",
			lamps_db=r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data\topographic-source-data.gpkg",
			lamps_layer="lagoon_poly",
		),
		LayerPair(
			lds_db=r"C:\Data\Topo50\kart-source\Latest\nz-lake_polygons-topo-150k\nz-lake_polygons-topo-150k.gpkg",
			lds_layer="nz_lake_polygons_topo_150k",
			lamps_db=r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data\topographic-source-data.gpkg",
			lamps_layer="lake_poly",
		),
		
]

def read_layer(path: str, layer_name: str) -> gpd.GeoDataFrame:
	"""Read a geospatial layer and require the matching identifier."""
	layer = gpd.read_file(path, layer=layer_name, fid_as_index=True)
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
		columns={"t50_fid": "lds_t50_fid"}
	)
	lamps = lamps.rename_geometry("lamps_geometry").rename(
		columns={"t50_fid": "lamps_t50_fid"}
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
	output_columns = ["lds_fid", "lamps_fid", "name", "gazfeatid", "geometry"]
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
			query_geometry = lds_geometry
			predicate = "contains"
		else:
			query_geometry = lds_geometry
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
					"lds_fid": lds_row["lds_t50_fid"],
					"lamps_fid": lamps_row["lamps_t50_fid"],
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

	return gpd.GeoDataFrame(
		records,
		columns=output_columns,
		geometry="geometry",
		crs=lds_only.crs,
	)


if __name__ == "__main__":
	process_pairs(LAYER_PAIRS, OUTPUT_DIRECTORY)

