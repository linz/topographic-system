"""Export GPKG MultiPolygon features that contain more than one polygon."""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyogrio


DEFAULT_GPKG_PATH = Path(r"C:\Data\toposource\topographic-data\topographic-data.gpkg")
DEFAULT_LAYERS = ("vegetation", "landcover")
DEFAULT_OUTPUT_PATH = Path(r"C:\Temp\export\multipart_count_features.csv")
DEFAULT_FEATURE_OUTPUT_DIRECTORY = Path(r"C:\Temp\export\multipart_feature_geojson")
OUTPUT_FIELDS = ["id", "t50_fid", "type", "subtype", "created_at"]


def find_multipart_features(gpkg_path: Path, layer_name: str) -> pd.DataFrame:
    """Return features whose MultiPolygon geometry has more than one part."""
    source_fields = set(pyogrio.read_info(gpkg_path, layer=layer_name)["fields"])
    features = pyogrio.read_dataframe(
        gpkg_path,
        layer=layer_name,
        columns=[field_name for field_name in OUTPUT_FIELDS if field_name in source_fields],
        read_geometry=True,
    )
    for field_name in OUTPUT_FIELDS:
        if field_name not in features:
            features[field_name] = pd.NA
    polygon_counts = features.geometry.map(
        lambda geometry: len(geometry.geoms)
        if geometry is not None and geometry.geom_type == "MultiPolygon"
        else 0
    )
    multipart_features = features.loc[polygon_counts > 1, OUTPUT_FIELDS].copy()
    multipart_features["t50_fid"] = pd.to_numeric(
        multipart_features["t50_fid"], errors="raise"
    ).astype("Int64")
    multipart_features.insert(0, "layer", layer_name)
    multipart_features["polygon_count"] = polygon_counts.loc[multipart_features.index]
    return multipart_features


def write_multipart_feature_geojsons(
    gpkg_path: Path, layer_name: str, output_directory: Path
) -> int:
    """Write each qualifying multipart source feature to its own GeoJSON file."""
    features = pyogrio.read_dataframe(gpkg_path, layer=layer_name, read_geometry=True)
    polygon_counts = features.geometry.map(
        lambda geometry: len(geometry.geoms)
        if geometry is not None and geometry.geom_type == "MultiPolygon"
        else 0
    )
    multipart_features = features.loc[polygon_counts > 1]
    output_directory.mkdir(parents=True, exist_ok=True)

    for _, feature in multipart_features.iterrows():
        feature_id = str(feature["id"])
        t50_fid = int(feature["t50_fid"])
        output_path = output_directory / f"{layer_name}_{t50_fid}_{feature_id}.geojson"
        gpd.GeoDataFrame([feature], crs=features.crs).to_file(
            output_path, driver="GeoJSON"
        )

    return len(multipart_features)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export MultiPolygon features with more than one polygon part."
    )
    parser.add_argument("--gpkg", type=Path, default=DEFAULT_GPKG_PATH)
    parser.add_argument("--layers", nargs="+", default=DEFAULT_LAYERS)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument(
        "--feature-output-directory",
        type=Path,
        default=DEFAULT_FEATURE_OUTPUT_DIRECTORY,
    )
    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()
    multipart_features = pd.concat(
        [
            find_multipart_features(arguments.gpkg, layer_name)
            for layer_name in arguments.layers
        ],
        ignore_index=True,
    )
    arguments.output_path.parent.mkdir(parents=True, exist_ok=True)
    multipart_features.to_csv(arguments.output_path, index=False)
    written_feature_count = sum(
        write_multipart_feature_geojsons(
            arguments.gpkg, layer_name, arguments.feature_output_directory
        )
        for layer_name in arguments.layers
    )
    print(
        f"Wrote {len(multipart_features)} multipart feature(s) to "
        f"{arguments.output_path}"
    )
    print(
        f"Wrote {written_feature_count} GeoJSON feature file(s) to "
        f"{arguments.feature_output_directory}"
    )


if __name__ == "__main__":
    main()