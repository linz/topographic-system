import argparse
from pathlib import Path

from export_from_gpkg_geoparquet import GeoPackageGeoParquetExporter


def find_gpkg_files(input_path: Path) -> list[Path]:
    if input_path.is_file():
        if input_path.suffix.lower() != ".gpkg":
            raise ValueError(f"Input file is not a GeoPackage: {input_path}")
        return [input_path]

    if input_path.is_dir():
        return sorted(path for path in input_path.rglob("*") if path.is_file() and path.suffix.lower() == ".gpkg")

    raise FileNotFoundError(f"Input path not found: {input_path}")


def get_output_folder(input_path: Path, gpkg_path: Path, output_path: Path, gpkg_count: int) -> Path:
    if gpkg_count == 1 and input_path.is_file():
        return output_path

    relative_gpkg = gpkg_path.relative_to(input_path) if input_path.is_dir() else Path(gpkg_path.name)
    return output_path / relative_gpkg.with_suffix("")


def run_export(input_path: Path, output_path: Path, layers: list[str] | None = None) -> None:
    gpkg_files = find_gpkg_files(input_path)
    if not gpkg_files:
        print(f"No GeoPackage files found in {input_path}")
        return

    for gpkg_path in gpkg_files:
        gpkg_output_path = get_output_folder(input_path, gpkg_path, output_path, len(gpkg_files))
        exporter = GeoPackageGeoParquetExporter(gpkg_path, gpkg_output_path, layers=layers)
        exporter.export()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export one GeoPackage file, or all GeoPackages in a folder tree, to GeoParquet."
    )
    parser.add_argument("input_path", type=Path, help="GeoPackage file or folder containing GeoPackages")
    parser.add_argument("output_path", type=Path, help="Folder to write GeoParquet files into")
    parser.add_argument(
        "--layers",
        nargs="+",
        help="Optional layer names to export. Defaults to all layers in each GeoPackage.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_export(args.input_path, args.output_path, args.layers)