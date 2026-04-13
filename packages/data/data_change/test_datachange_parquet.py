import os
import json
from pathlib import Path

import pandas as pd

from packages.data.data_change.generate_datachange import GeoParquetTableDiffComparer


def geojson_to_geoparquet(geojson_path: str, output_parquet_path: str) -> str:
    """Read a GeoJSON FeatureCollection and write it as a parquet file for tests."""
    with open(geojson_path, encoding="utf-8") as f:
        data = json.load(f)

    features = data.get("features", [])
    rows = []
    for feature in features:
        properties = feature.get("properties") or {}
        row = dict(properties)
        row["geometry"] = json.dumps(feature.get("geometry"), separators=(",", ":"))
        rows.append(row)

    df = pd.DataFrame(rows)
    Path(output_parquet_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_parquet_path, index=False)
    return output_parquet_path


def setup_test_release_parquet_files(base_dir: Path, geojson_file: str, output_base_dir: Path, output_foldername: str, output_filename: str) -> str:
    """Create release folders and write one track_line parquet file per release from GeoJSON fixtures."""

    current_release_path = output_base_dir / output_foldername

    current_release_path.mkdir(parents=True, exist_ok=True)

    geojson_to_geoparquet(
        geojson_path=str(base_dir / geojson_file),
        output_parquet_path=str(current_release_path / output_filename),
    )

    return str(current_release_path)


def build_comparer(base_dir: Path, parquet_filename: str, testfile: str, output_foldername: str, logs_folder: str) -> GeoParquetTableDiffComparer:
    """Set up local fixture inputs and return a configured comparer instance."""

    current_release_path = setup_test_release_parquet_files(base_dir, testfile, output_base_dir, output_foldername, parquet_filename)

    os.makedirs(logs_folder, exist_ok=True)

    return GeoParquetTableDiffComparer(
        current_release_name="release2",
        previous_release_name="release1",
        release_date="2025-09-25",
        current_release_path=current_release_path,
        previous_release_path=previous_release_path,
        use_hive_partitioning=False,
        change_logs_path=logs_folder
    )


if __name__ == "__main__":

    parquet_filename = "track_line.parquet"
    base_dir = Path(__file__).resolve().parent
    output_base_dir = Path(r"c:\temp\datachanges_test")
    

    basefile = "track_line_base.geojson"
    previous_release_path = setup_test_release_parquet_files(base_dir, basefile, output_base_dir, "release1", parquet_filename)

    print("TESTING ADD/DELETE SCENARIO")
    testfile = "track_line_add_del.geojson"
    logs_folder = r"c:\temp\datachanges_adddel"
    build_comparer(base_dir, parquet_filename, testfile, "release2_adddel", logs_folder).run()

    print("TESTING GEOMETRY UPDATES SCENARIO")
    testfile = "track_line_geom_updates.geojson"
    logs_folder = r"c:\temp\datachanges_geom_updates"
    build_comparer(base_dir, parquet_filename, testfile, "release2_geom_updates", logs_folder).run()

    print("TESTING ATTRIBUTE UPDATES SCENARIO")
    testfile = "track_line_updates.geojson"
    logs_folder = r"c:\temp\datachanges_updates"
    build_comparer(base_dir, parquet_filename, testfile, "release2_updates", logs_folder).run()