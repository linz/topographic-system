import argparse
import json
import os
from pathlib import Path

import pandas as pd
from export_to_lds_model import EXPORT_EXTENTS, ExportToLDSModel

DEFAULT_OUTPUT_FOLDER = Path(r"C:\temp\export\lds_model")
DATA_PATH_FILE = Path(__file__).resolve().parent / "topo50_schemas" / "data_path.json"


def load_data_paths(data_path_file=DATA_PATH_FILE):
    with Path(data_path_file).open(encoding="utf-8") as file:
        config = json.load(file)
    locations = config.get("locations")
    default = config.get("default")
    if not isinstance(locations, dict) or not locations:
        raise ValueError(f"Expected non-empty locations in {data_path_file}")
    if default not in locations:
        raise ValueError(f"Default location {default!r} is not configured")
    for name, location in locations.items():
        if not isinstance(location, dict):
            raise ValueError(f"Data location {name!r} must be an object")
        for key in ("url", "data_path"):
            value = location.get(key)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"Data location {name!r} requires a non-empty {key}")
    return default, locations


def parse_args():
    default_location, locations = load_data_paths()
    parser = argparse.ArgumentParser(description="Export LDS model layers")
    parser.add_argument(
        "extent",
        choices=EXPORT_EXTENTS,
        help="Export extent to process",
    )
    parser.add_argument(
        "data_location",
        nargs="?",
        choices=locations,
        default=default_location,
        help="Configured Parquet location (default: %(default)s)",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=DEFAULT_OUTPUT_FOLDER,
        help="LDS export root directory (default: %(default)s)",
    )
    return parser.parse_args()


def process_source(extent, data_location=None, output_path=DEFAULT_OUTPUT_FOLDER):
    script_folder = Path(__file__).resolve().parent
    config_directory = script_folder / "topo50_schemas" / "config"
    default_location, locations = load_data_paths()
    location_name = data_location or default_location
    location = locations.get(location_name)
    if location is None:
        raise ValueError(f"Unknown data location: {location_name}")

    start_time = pd.Timestamp.now()
    area_output_folder = Path(output_path) / extent
    os.makedirs(area_output_folder, exist_ok=True)

    exporter = ExportToLDSModel(
        config_directory,
        location["url"],
        location["data_path"],
        area_output_folder,
    )
    exporter.process_layers(extent)
    end_time = pd.Timestamp.now()
    print(f"Processing time: {end_time - start_time}")


def main():
    args = parse_args()
    process_source(args.extent, args.data_location, args.output_path)


if __name__ == "__main__":
    main()
