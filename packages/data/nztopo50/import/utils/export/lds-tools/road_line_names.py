import csv
from pathlib import Path

import geopandas as gpd
import pyogrio

db_path = Path(r"C:\Data\Topo50\kart-source\nz_addresses\nz-roads-pilot")
gpkg_db = "nz-roads-pilot.gpkg"
layer = "nz_roads_pilot"

base_folder = Path(r"C:\Temp\export\lds_model")
source_data_file = base_folder / "road_line_names.csv"
output_data_file = base_folder / "road_line_names_export.csv"
output_geoparquet_file = base_folder / "road_line_names_export.parquet"


def load_roads():
	roads = pyogrio.read_dataframe(
		db_path / gpkg_db,
		layer=layer,
		columns=["road_name_body", "full_road_name"],
		read_geometry=True,
		fid_as_index=True,
	)
	roads.index.name = "road_id"
	roads["road_name"] = roads["road_name_body"]
	numeric_road_names = (
		roads["road_name"].astype("string").str.fullmatch(r"\d+").fillna(False)
	)
	roads.loc[numeric_road_names, "road_name"] = roads.loc[
		numeric_road_names, "full_road_name"
	]
	return roads


def export_road_names():
	roads = load_roads()
	geoparquet_rows = []

	with source_data_file.open(newline="", encoding="utf-8-sig") as source_file:
		reader = csv.DictReader(source_file)
		if reader.fieldnames != ["t50_fid", "rns_sufi", "name"]:
			raise ValueError(
				"Expected CSV columns: t50_fid,rns_sufi,name; "
				f"found: {reader.fieldnames}"
			)

		with output_data_file.open("w", newline="", encoding="utf-8") as output_file:
			writer = csv.DictWriter(
				output_file,
				fieldnames=["t50_fid", "rns_sufi", "name"],
			)
			writer.writeheader()
			for row in reader:
				road_id = int(row["rns_sufi"])
				road = roads.loc[road_id] if road_id in roads.index else None
				road_name = road["road_name"] if road is not None else None
				row["name"] = (
					str(road_name).upper() if road_name else "no road name"
				)
				writer.writerow(row)
				if road is not None:
					geoparquet_rows.append(
						{
							"t50_fid": int(row["t50_fid"]),
							"rns_sufi": road_id,
							"name": row["name"],
							"geometry": road["geometry"],
						}
					)

	geoparquet = gpd.GeoDataFrame(
		geoparquet_rows,
		columns=["t50_fid", "rns_sufi", "name", "geometry"],
		geometry="geometry",
		crs=roads.crs,
	)
	geoparquet.to_parquet(output_geoparquet_file, index=False)

	print(f"Wrote {output_data_file}")
	print(f"Wrote {output_geoparquet_file}")


if __name__ == "__main__":
	export_road_names()