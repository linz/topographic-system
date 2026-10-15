from collections.abc import Iterable
from pathlib import Path

import geopandas as gpd  # type: ignore
import pyogrio


class GeoPackageGeoParquetExporter:
    def __init__(
        self,
        gpkg_path: str | Path,
        output_path: str | Path,
        layers: Iterable[str] | None = None,
        row_group_size: int = 50000,
    ) -> None:
        self.gpkg_path = Path(gpkg_path)
        self.output_path = Path(output_path)
        self.layers = list(layers) if layers else []
        self.row_group_size = row_group_size

    def list_layers(self) -> list[str]:
        return [layer_name for layer_name, _geometry_type in pyogrio.list_layers(self.gpkg_path)]

    def export(self) -> list[Path]:
        if not self.gpkg_path.exists():
            raise FileNotFoundError(f"GeoPackage not found: {self.gpkg_path}")

        self.output_path.mkdir(parents=True, exist_ok=True)
        exported_paths = []
        layers = self.layers or self.list_layers()

        for layer in layers:
            exported_paths.append(self.export_layer(layer))

        return exported_paths

    def export_layer(self, layer: str) -> Path:
        print(f"Processing {self.gpkg_path.name}: {layer}")
        gdf = gpd.read_file(self.gpkg_path, layer=layer)
        export_path = self.output_path / f"{layer}.parquet"
        gdf.to_parquet(
            export_path,
            engine="pyarrow",
            compression="zstd",  # type: ignore[arg-type]
            write_covering_bbox=True,
            row_group_size=self.row_group_size,
        )
        print(f"Exported {layer} to {export_path}")
        return export_path
