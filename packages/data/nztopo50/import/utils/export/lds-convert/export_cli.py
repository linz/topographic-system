import argparse
import os
from pathlib import Path

import pandas as pd

from export_to_lds_model import EXPORT_EXTENTS, ExportToLDSModel


DEFAULT_OUTPUT_FOLDER = Path(r"C:\temp\export\lds_model")
DEFAULT_SOURCE_FORMAT = "parquet"
DATA_SOURCE_PATHS = {
    "topo-data": Path(r"C:\Data\temp\topographic-data-dev-67"),
    "contour-data": Path(r"C:\Data\temp\topographic-contour-data"),
    "product-data": Path(r"C:\Data\temp\topographic-product-data"),
}


def parse_args():
    parser = argparse.ArgumentParser(description="Export LDS model layers")
    parser.add_argument(
        "data_source",
        choices=DATA_SOURCE_PATHS,
        help="Data source to process",
    )
    parser.add_argument(
        "extent",
        choices=EXPORT_EXTENTS,
        help="Export extent to process",
    )
    parser.add_argument(
        "path",
        nargs="?",
        type=Path,
        help="Optional path override; otherwise the configured default is used",
    )
    return parser.parse_args()


def process_source(data_source, extent, path=None):
    script_folder = Path(__file__).resolve().parent
    config_directory = script_folder / "topo50_schemas" / "config"
    output_folder = DEFAULT_OUTPUT_FOLDER
    source_path = path or DATA_SOURCE_PATHS[data_source]

    start_time = pd.Timestamp.now()
    area_output_folder = output_folder / extent
    os.makedirs(area_output_folder, exist_ok=True)

    exporter = ExportToLDSModel(
        config_directory,
        source_path,
        area_output_folder,
        source_format=DEFAULT_SOURCE_FORMAT,
    )
    exporter.process_layers(extent, source_name=data_source)
    end_time = pd.Timestamp.now()
    print(f"Processing time: {end_time - start_time}")


def main():
    args = parse_args()
    process_source(args.data_source, args.extent, args.path)


if __name__ == "__main__":
    main()
