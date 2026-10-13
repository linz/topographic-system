# GeoParquet Export Scripts

Utilities for exporting spatial data from GeoPackage files or PostgreSQL tables
to GeoParquet.

Run commands from `packages/data/nztopo50/import` so the project environment and
local imports resolve correctly.

## Prerequisites

Install the project dependencies and run scripts through `uv`:

```powershell
uv sync
```

The exporters use GeoPandas, Pyogrio, PyArrow, and Zstandard compression. The
PostgreSQL exporter also requires a reachable PostgreSQL database and the
project's Psycopg and SQLAlchemy dependencies.

## GeoPackage Export

`run_export_gkpg.py` is the command-line entry point.
`export_from_gpkg_geoparquet.py` contains the reusable
`GeoPackageGeoParquetExporter` class.

Each GeoPackage layer is written to a separate `<layer>.parquet` file with:

- Zstandard compression
- GeoParquet covering bounding boxes
- 50,000 rows per row group
- Source geometry and CRS metadata preserved by GeoPandas

### Export One GeoPackage

```powershell
uv run python utils\export\parquet\run_export_gkpg.py `
  C:\Data\toposource\topographic-data\topographic-data.gpkg `
  C:\Data\temp\topo-parquet\topographic-data
```

The output folder contains one Parquet file per layer:

```text
C:\Data\temp\topo-parquet\topographic-data\
  landuse.parquet
  road_cl.parquet
  structure_point.parquet
```

### Export Selected Layers

```powershell
uv run python utils\export\parquet\run_export_gkpg.py `
  C:\Data\toposource\topographic-data\topographic-data.gpkg `
  C:\Data\temp\topo-parquet\topographic-data `
  --layers landuse road_cl
```

Layer names must exist in each GeoPackage being processed.

### Export a Folder Tree

```powershell
uv run python utils\export\parquet\run_export_gkpg.py `
  C:\Data\toposource `
  C:\Data\temp\topo-parquet
```

The runner recursively finds every `.gpkg` file. For folder input, it creates a
separate output directory for each GeoPackage while preserving the relative
input path and removing the `.gpkg` suffix.

For example:

```text
Input:  C:\Data\toposource\release\topographic-data.gpkg
Output: C:\Data\temp\topo-parquet\release\topographic-data\<layer>.parquet
```

### CLI Reference

```text
run_export_gkpg.py INPUT_PATH OUTPUT_PATH [--layers LAYER [LAYER ...]]
```

- `INPUT_PATH`: One `.gpkg` file or a directory searched recursively.
- `OUTPUT_PATH`: Directory where GeoParquet files are written.
- `--layers`: Optional list of layer names. All layers are exported by default.

Existing Parquet files with matching names are replaced.

## Python API

Use the exporter class when calling the workflow from another Python module:

```python
from utils.export.parquet.export_from_gpkg_geoparquet import (
    GeoPackageGeoParquetExporter,
)

exporter = GeoPackageGeoParquetExporter(
    gpkg_path=r"C:\Data\toposource\topographic-data.gpkg",
    output_path=r"C:\Data\temp\topo-parquet\topographic-data",
    layers=["landuse", "road_cl"],
    row_group_size=50_000,
)
exported_paths = exporter.export()
```

`export()` returns the paths of the Parquet files it created.

## PostgreSQL Export

`export_to_geoparquet_geopandas.py` exports PostgreSQL schemas to GeoParquet.
It is currently configured in its `if __name__ == "__main__"` block rather than
through command-line arguments.

Before running it, review and update:

- `export_topo_data` and `export_carto_data`
- `DB_PARAMS` and `db_connection_url`
- `schema`
- `release_date`
- `export_folder`
- Geometry column names and CRS assumptions

Then run:

```powershell
uv run python utils\export\parquet\export_to_geoparquet_geopandas.py
```

Do not commit real database passwords. Prefer environment-provided credentials
when adapting this script for shared or automated use.

## Notes

- The GeoPackage runner does not merge layers; each layer becomes one file.
- Output directories are created automatically.
- A missing input path raises an error.
- An input directory containing no GeoPackages prints a message and exits.
- Export errors stop the run; partially written outputs from earlier layers may
  remain in the output directory.
