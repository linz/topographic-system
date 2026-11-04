"""Print deleted road-line IDs that are absent from the removal changelog."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import pyogrio


DEFAULT_RELEASE_DIRECTORY = Path(r"C:\Temp\data-changes\release67")
DEFAULT_CSV_PATH = DEFAULT_RELEASE_DIRECTORY / "roads-changeset.csv"
DEFAULT_PARQUET_PATH = DEFAULT_RELEASE_DIRECTORY / "release67_road_line_removed_changelog.parquet"
DEFAULT_GPKG_PATH = Path(r"C:\Data\toposource\topographic-data-66\topographic-data.gpkg")


def normalise_values(values: pd.Series) -> pd.Series:
    """Return non-null string values suitable for joining data sources."""
    return values.astype("string").str.strip()


def read_deleted_t50_fids(csv_path: Path) -> set[str]:
    changes = pd.read_csv(csv_path, usecols=["change", "t50_fid"], dtype="string")
    deleted_t50_fids = normalise_values(changes.loc[changes["change"].str.strip() == "DELETE", "t50_fid"])
    return set(deleted_t50_fids.dropna())


def find_missing_ids(csv_path: Path, gpkg_path: Path, parquet_path: Path) -> list[str]:
    deleted_t50_fids = read_deleted_t50_fids(csv_path)
    road_lines = pyogrio.read_dataframe(
        gpkg_path,
        layer="road_line",
        columns=["id", "t50_fid"],
        read_geometry=False,
    )
    road_lines["t50_fid"] = normalise_values(road_lines["t50_fid"])
    matching_ids = normalise_values(
        road_lines.loc[road_lines["t50_fid"].isin(deleted_t50_fids), "id"]
    ).dropna()

    removed_ids = normalise_values(pd.read_parquet(parquet_path, columns=["id"])["id"])
    removed_id_set = set(removed_ids.dropna())
    return list(dict.fromkeys(id_value for id_value in matching_ids if id_value not in removed_id_set))


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print deleted road-line IDs that are absent from a removal changelog."
    )
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV_PATH, help="Road changeset CSV path.")
    parser.add_argument("--gpkg", type=Path, default=DEFAULT_GPKG_PATH, help="Source GeoPackage path.")
    parser.add_argument(
        "--parquet", type=Path, default=DEFAULT_PARQUET_PATH, help="Removal changelog parquet path."
    )
    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()
    missing_ids = find_missing_ids(arguments.csv, arguments.gpkg, arguments.parquet)
    print(*missing_ids, sep="\n")
    print(f"{len(missing_ids)} ID(s) missing from the removal changelog.")


if __name__ == "__main__":
    main()