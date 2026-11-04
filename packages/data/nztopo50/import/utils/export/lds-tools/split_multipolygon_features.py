"""Split GeoJSON multipart line or polygon features into single parts."""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd

DEFAULT_INPUT_DIRECTORY = Path(r"C:\Temp\export\multipart_feature_geojson")
DEFAULT_OUTPUT_DIRECTORY = DEFAULT_INPUT_DIRECTORY / "single"
SUPPORTED_PART_TYPES = {"LineString", "Polygon"}


def split_multipart_features(input_path: Path) -> gpd.GeoDataFrame:
    """Return multipart line or polygon features split into single parts."""
    features = gpd.read_file(input_path)
    parts = features.explode(index_parts=False, ignore_index=True)
    invalid_parts = parts.geometry.notna() & ~parts.geometry.geom_type.isin(
        SUPPORTED_PART_TYPES
    )
    if invalid_parts.any():
        invalid_types = sorted(parts.loc[invalid_parts].geometry.geom_type.unique())
        raise ValueError(
            "Expected only line or polygon geometries; "
            f"found {invalid_types}"
        )
    return parts


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Split GeoJSON MultiLineString or MultiPolygon files into individual parts."
        )
    )
    parser.add_argument(
        "--input-directory", type=Path, default=DEFAULT_INPUT_DIRECTORY
    )
    parser.add_argument(
        "--output-directory", type=Path, default=DEFAULT_OUTPUT_DIRECTORY
    )
    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()
    input_paths = sorted(arguments.input_directory.glob("*.geojson"))
    if not input_paths:
        raise ValueError(f"No GeoJSON files found in {arguments.input_directory}")

    arguments.output_directory.mkdir(parents=True, exist_ok=True)
    part_count = 0
    for input_path in input_paths:
        parts = split_multipart_features(input_path)
        output_path = arguments.output_directory / input_path.name
        parts.to_file(output_path, driver="GeoJSON")
        part_count += len(parts)
        print(f"Wrote {len(parts)} geometry part(s) to {output_path}")

    print(f"Processed {len(input_paths)} file(s), writing {part_count} geometry part(s)")


if __name__ == "__main__":
    main()
