"""Split GeoJSON MultiPolygon features into one feature per Polygon."""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd


DEFAULT_INPUT_DIRECTORY = Path(r"C:\Temp\export\multipart_feature_geojson")
DEFAULT_OUTPUT_DIRECTORY = DEFAULT_INPUT_DIRECTORY / "single"


def split_multipolygons(input_path: Path) -> gpd.GeoDataFrame:
    """Return the input features with each MultiPolygon split into Polygons."""
    features = gpd.read_file(input_path)
    polygons = features.explode(index_parts=False, ignore_index=True)
    non_polygons = polygons.geometry.notna() & (polygons.geometry.geom_type != "Polygon")
    if non_polygons.any():
        invalid_types = sorted(polygons.loc[non_polygons].geometry.geom_type.unique())
        raise ValueError(f"Expected only Polygon or MultiPolygon geometries; found {invalid_types}")
    return polygons


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Split GeoJSON MultiPolygon files into individual Polygon features."
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
    polygon_count = 0
    for input_path in input_paths:
        polygons = split_multipolygons(input_path)
        output_path = arguments.output_directory / input_path.name
        polygons.to_file(output_path, driver="GeoJSON")
        polygon_count += len(polygons)
        print(f"Wrote {len(polygons)} Polygon feature(s) to {output_path}")

    print(f"Processed {len(input_paths)} file(s), writing {polygon_count} Polygon feature(s)")


if __name__ == "__main__":
    main()