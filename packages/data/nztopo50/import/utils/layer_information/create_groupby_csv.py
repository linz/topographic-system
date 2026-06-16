from pathlib import Path
import pandas as pd

output_file = Path(r"C:\Data\model\schema_columns_groupby.csv")

TABLE_COLUMNS = {
	"airport": ["type"],
	"bridge_line": ["type", "bridge_use", "bridge_use2", "construction_type", "status"],
	"building": ["type", "building_use", "status"],
	"building_point": ["type", "building_use", "status"],
	"coastline": ["type", "coastline_type"],
	"contour": ["type", "definition", "designation", "formation"],
	"descriptive_text": ["type"],
	"fence_line": ["type"],
	"ferry_crossing": ["type"],
	"geographic_name": ["type"],
	"island": ["type"],
	"landcover": ["type", "subtype"],
	"landcover_line": ["type"],
	"landcover_point": ["type", "display"],
	"landuse": ["type", "landuse_use", "subtype", "status", "substance_extracted"],
	"landuse_line": ["type", "landuse_use", "subtype"],
	"marine": ["type", "composition"],
	"nztopo50_map_sheet": [
		"type",
		"sheet_code",
		"sheet_name",
		"edition",
		"example_class",
		"example_name",
		"revised",
	],
	"utility_line": ["type", "utility_use", "support_type", "status", "visibility"],
	"utility_point": ["type"],
	"place_point": ["type", "place_type", "status", "composition", "substance_extracted"],
	"railway_line": ["type", "railway_use", "track_type", "vehicle_type", "status"],
	"railway_station": ["type"],
	"relief": ["type"],
	"relief_line": ["type", "relief_use"],
	"relief_point": ["type", "name", "display"],
	"residential_area": ["type"],
	"road_line": ["type", "hierarchy", "status", "lane_count", "surface", "way_count", "width_indicator"],
	"runway": ["type", "runway_use", "status", "surface"],
	"structure": ["lid_type", "subtype", "species", "status", "stored_item"],
	"structure_line": ["type", "structure_use", "species", "status", "material", "material_conveyed", "restrictions"],
	"structure_point": ["type", "structure_use", "subtype", "status", "material", "restrictions", "stored_item", "wreck_of"],
	"track_line": ["type", "track_use", "track_type", "status"],
	"transport_point": ["type"],
	"trig_point": ["type", "trig_type"],
	"tunnel_line": ["type", "tunnel_use", "tunnel_use2", "subtype"],
	"water": ["type", "water_use", "hierarchy", "perennial"],
	"water_line": ["type"],
	"water_point": ["type", "temperature_indicator"],
	"vegetation_point": ["type"],
}


def build_groupby_rows(table_columns: dict[str, list[str]]) -> list[dict[str, str]]:
	rows = []
	for table_name, columns in table_columns.items():
		for column_name in columns:
			rows.append(
				{
					"table": table_name,
					"columns": column_name,
					"group_by": "TRUE",
				}
			)
	return rows


def main() -> None:
	rows = build_groupby_rows(TABLE_COLUMNS)
	df = pd.DataFrame(rows, columns=["table", "columns", "group_by"])
	df.to_csv(output_file, index=False)
	print(f"Saved {len(rows)} rows to {output_file}")


if __name__ == "__main__":
	main()
