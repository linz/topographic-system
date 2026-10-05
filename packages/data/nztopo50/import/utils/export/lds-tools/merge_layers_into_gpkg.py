import argparse
from pathlib import Path

import geopandas as gpd
import pyogrio


folder = Path(r"C:\Data\Topo50\kart-source\nz-islands")
output = folder / "all_islands.gpkg"
log_file = folder / "exported_layers.log"


def merge_layers(folder_prefix: str | None = None) -> None:
	input_files = sorted(
		input_file
		for subfolder in folder.iterdir()
		if subfolder.is_dir()
		and (folder_prefix is None or subfolder.name.startswith(folder_prefix))
		for input_file in subfolder.glob("*.gpkg")
	)
	if not input_files:
		group = f" starting with {folder_prefix!r}" if folder_prefix else ""
		raise FileNotFoundError(f"No source GeoPackages found in {folder}{group}")

	if output.exists():
		output.unlink()

	with log_file.open("w", encoding="utf-8") as exported_log:
		layer_index = 0
		for input_file in input_files:
			layers = pyogrio.list_layers(input_file)
			if len(layers) == 0:
				raise ValueError(f"No layers found in {input_file}")

			for layer_name, _geometry_type in layers:
				print(f"Copying {layer_name} from {input_file}")
				layer = gpd.read_file(input_file, layer=layer_name)
				layer.to_file(
					output,
					layer=layer_name,
					driver="GPKG",
					mode="w" if layer_index == 0 else "a",
					engine="pyogrio",
				)
				exported_log.write(f"{input_file.name},{layer_name}\n")
				layer_index += 1


if __name__ == "__main__":
	parser = argparse.ArgumentParser(
		description="Merge GeoPackage layers from the island dataset folders."
	)
	parser.add_argument(
		"folder_prefix",
		nargs="?",
		help="Only process subfolders whose names start with this value.",
	)
	args = parser.parse_args()
	merge_layers(args.folder_prefix)

