# Export Utilities

Scripts for exporting NZ Topo50 data to various formats.

## Files

### `db_common_connection.py`
Shared database connection class (`DBTables`) used by export scripts to connect to PostgreSQL and list schema tables.

### `export_model_jsonschema.py`
Generates JSON Schema definitions from PostgreSQL table structures. Maps PostgreSQL column types to JSON Schema types and exports schema metadata for each layer.

---

## Subdirectories

### `fids/`
Scripts to scan shapefiles and find the maximum `t50_fid` value across all files, writing the results to a JSON file. Useful for tracking feature ID ranges before import.

| File | Description |
|------|-------------|
| `create_max_id_json.py` | Reads shapefiles using `geopandas` to find max `t50_fid` per file |
| `create_max_id_json_duckdb.py` | Faster alternative using `duckdb` with spatial extension |

---

### `LDS/`
Scripts for exporting data to the LINZ Data Service (LDS) shapefile format.

| File | Description |
|------|-------------|
| `create_lds_field_map.py` | Reads field definitions from shapefiles and exports a JSON field map to enforce consistent field types during LDS export |
| `export_to_lds_model.py` | Exports NZ Topo50 layers from PostgreSQL to LDS-format shapefiles using layer info, field mappings, and a master schema JSON |
| `Release62_NZ50_Schemas/nztopo50_lds_schemas.json` | Master schema file defining field formats for Release 62 NZ Topo50 LDS shapefiles |

---

### `parquet/`
Scripts for exporting spatial data to GeoParquet format.

| File | Description |
|------|-------------|
| `export_from_gpkg_geoparquet.py` | Exports layers from a GeoPackage (`.gpkg`) to GeoParquet files using `geopandas` |
| `run_export_gkpg.py` | Runs GeoPackage to GeoParquet exports for one `.gpkg` file or every `.gpkg` found under a folder |
| `export_to_geoparquet_geopandas.py` | Exports layers from PostgreSQL to GeoParquet files using `geopandas` and `sqlalchemy` |

Run from the `packages/data/nztopo50/import` folder.

Export one GeoPackage:

```powershell
uv run python utils\export\parquet\run_export_gkpg.py C:\Data\toposource\topographic-data-dev\topographic-data-dev.gpkg C:\Data\temp\topo-parquet
```

Export every GeoPackage found in a folder and its subfolders:

```powershell
uv run python utils\export\parquet\run_export_gkpg.py C:\Data\toposource C:\Data\temp\topo-parquet
```

Export selected layers only:

```powershell
uv run python utils\export\parquet\run_export_gkpg.py C:\Data\toposource\topographic-data-dev\topographic-data-dev.gpkg C:\Data\temp\topo-parquet --layers landuse road_cl
```
