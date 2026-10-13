import argparse
import sqlite3
from pathlib import Path

import geopandas as gpd
import pyogrio


DEFAULT_SOURCE_FOLDER = Path(
	r"C:\Data\Topo50\kart-topographic-source-data\release67-lamps-extra-exports\20260922"
)

DEFAULT_SOURCE_FOLDER = Path(
	r"C:\Data\Topo50\Release67_NZ50_Shape\release67-ftp-water-exports"
)

DEFAULT_SOURCE_FOLDER = Path(
	r"C:\Data\Topo50\kart-topographic-source-data\release67-lamps-extra-exports-road"
)

def geometry_column_name(gpkg_path: Path, layer_name: str) -> str | None:
	with sqlite3.connect(gpkg_path) as connection:
		row = connection.execute(
			"""
			SELECT column_name
			FROM gpkg_geometry_columns
			WHERE table_name = ?
			""",
			(layer_name,),
		).fetchone()
	return row[0] if row else None


def read_shapefile(shapefile: Path) -> gpd.GeoDataFrame:
	try:
		return gpd.read_file(shapefile, engine="pyogrio")
	except UnicodeDecodeError:
		print(f"Retrying {shapefile.name} with Windows-1252 encoding")
		return gpd.read_file(shapefile, engine="pyogrio", encoding="cp1252")


def import_shapefiles(source_folder: Path, output_path: Path | None = None) -> Path:
	if not source_folder.is_dir():
		raise NotADirectoryError(f"Shapefile folder not found: {source_folder}")

	shapefiles = sorted(source_folder.glob("*.shp"))
	if not shapefiles:
		raise FileNotFoundError(f"No shapefiles found in: {source_folder}")

	if output_path is None:
		output_path = source_folder / f"{source_folder.name}.gpkg"
	elif not output_path.is_absolute():
		output_path = source_folder / output_path

	output_path.parent.mkdir(parents=True, exist_ok=True)

	for shapefile in shapefiles:
		layer_name = shapefile.stem
		print(f"Reading {shapefile.name}")
		dataframe = read_shapefile(shapefile)

		if dataframe.geometry.name != "geometry":
			if "geometry" in dataframe.columns:
				raise ValueError(
					f"Cannot rename {shapefile.name} geometry column: "
					"a non-active 'geometry' column already exists"
				)
			dataframe = dataframe.rename_geometry("geometry")

		pyogrio.write_dataframe(
			dataframe,
			output_path,
			layer=layer_name,
			driver="GPKG",
			append=False,
			layer_options={"GEOMETRY_NAME": "geometry"},
		)

		written_geometry_column = geometry_column_name(output_path, layer_name)
		if written_geometry_column != "geometry":
			raise RuntimeError(
				f"Layer {layer_name!r} was written with geometry column "
				f"{written_geometry_column!r}, not 'geometry'"
			)
		print(f"Wrote layer {layer_name!r}: {len(dataframe)} feature(s)")

	print(f"Complete: imported {len(shapefiles)} layer(s) into {output_path}")
	return output_path


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description=(
			"Import every shapefile in a folder into one GeoPackage using "
			"'geometry' as the geometry column name. Existing layers with matching "
			"names are replaced."
		)
	)
	parser.add_argument(
		"source_folder",
		nargs="?",
		type=Path,
		default=DEFAULT_SOURCE_FOLDER,
		help=f"Folder containing shapefiles. Defaults to {DEFAULT_SOURCE_FOLDER}",
	)
	parser.add_argument(
		"--output",
		type=Path,
		help=(
			"Output GeoPackage path. Relative paths are created inside the source "
			"folder. Defaults to <source-folder-name>.gpkg."
		),
	)
	return parser.parse_args()


if __name__ == "__main__":
	args = parse_args()
	import_shapefiles(args.source_folder, args.output)
