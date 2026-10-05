# LDS Conversion

Convert the consolidated NZ Topo50 model into the legacy LDS shapefile layout.

Run all commands from:

```text
packages/data/nztopo50/import
```

## Components

| Path | Purpose |
|---|---|
| `export_to_lds_model.py` | Reads model GeoPackages or GeoParquet files and writes LDS shapefiles |
| `match_exports.py` | Compares sampled export attributes against Release 66 Kart GeoPackages |
| `topo50_schemas/nztopo50_lds_schemas.json` | Master LDS field and geometry definitions used to generate configs |
| `topo50_schemas/config/*.json` | Runtime routing, field mapping, and Fiona schema information, grouped by source model layer |
| `../lds-tools/create_lds_config.py` | Regenerates runtime configs from the legacy workbooks and master schema |

The exporter uses only the files under `topo50_schemas/config` at runtime. It
does not read the Excel workbooks or master schema JSON during an export.

## Setup

Create or update the project environment:

```powershell
uv sync
```

The exporter uses Fiona to enforce shapefile field widths and types. The project
is constrained to a Fiona-compatible Python version.

## Generate Runtime Configs

Regenerate configs after changing the master schema, layer workbook, or field
mapping workbook:

```powershell
uv run python utils\export\lds-tools\create_lds_config.py C:\Data\Model\layers_info.xlsx C:\Data\Model\lds_field_mapping.xlsx utils\export\lds-convert\topo50_schemas\nztopo50_lds_schemas.json utils\export\lds-convert\topo50_schemas\config
```

The generator:

1. Groups LDS targets by consolidated source model layer.
2. Uses the master schema as the authoritative target field order and type.
3. Resolves each target field to its model source field.
4. Writes one JSON file per source model layer.
5. Reports targets that have no usable schema or mapping.

Legacy workbook-only fields that are absent from the master schema are ignored.
Generation fails when a schema field has no source mapping.

Each target config contains:

- `target_layer`: output shapefile name.
- `schema_key`: corresponding master schema entry.
- `feature_type`: value used to select records from a consolidated layer.
- `geometry_type`: Fiona output geometry.
- `field_mappings`: ordered source field, target field, and Fiona target type.

## Prepare GeoParquet Sources

The default export mode is GeoParquet. Each configured source directory must
contain one file per consolidated model layer, named `<source_layer>.parquet`,
for example `landuse.parquet`, `road_line.parquet`, and
`structure_point.parquet`.

Convert the three model GeoPackages when needed:

```powershell
uv run python utils\export\parquet\run_export_gkpg.py C:\Data\toposource\topographic-data\topographic-data.gpkg C:\Data\temp\topographic-data
uv run python utils\export\parquet\run_export_gkpg.py C:\Data\toposource\topographic-contour-data\topographic-contour-data.gpkg C:\Data\temp\topographic-contour-data
uv run python utils\export\parquet\run_export_gkpg.py C:\Data\toposource\topographic-product-data\topographic-product-data.gpkg C:\Data\temp\topographic-product-data
```

## Configure the Export

Edit the `__main__` block in `export_to_lds_model.py`:

```python
source_format = "parquet"
database = r"C:\Data\temp\topographic-data"
contour_database = r"C:\Data\temp\topographic-contour-data"
product_database = r"C:\Data\temp\topographic-product-data"
output_folder = r"C:\Temp\export\lds_model"
```

To export every configured target:

```python
single_file = None
```

To export one target only, use its LDS shapefile name with or without `.shp`:

```python
single_file = "road_cl"
```

GeoPackage input is also supported. Set `source_format = "gpkg"` and provide
the corresponding GeoPackage file paths instead of directories.

## Run the Export

```powershell
uv run python utils\export\lds-convert\export_to_lds_model.py
```

Output is written to the configured `output_folder`. The run log is:

```text
<output_folder>/export_to_lds_model.log
```

Unreadable or missing source layers are logged and skipped so the remaining
outputs can continue.

## Export Processing

For each LDS target, the exporter:

1. Reads the configured consolidated source layer.
2. Filters by `type` when the source layer feeds multiple LDS targets.
3. Reconstructs legacy source fields from the current model.
4. Recreates macron and ASCII name fields where required.
5. Reorders and renames fields using the target config.
6. Builds the Fiona schema dynamically from `target_type` and `geometry_type`.
7. Converts model underscores back to spaces in schema-declared string fields.
8. Reprojects to EPSG:2193 and writes an UTF-8 shapefile.

Current special reverse mappings include:

- Road `rna_sufi` recovered from the `road_id` value in metadata.
- Bivouac material `building` restored to null.
- Defaulted exotic species `coniferous` restored to null.
- Tree name exceptions retained for known `t50_fid` values.

## Compare an Export

`match_exports.py` uses the Release 66 Kart source tree as master data. It
recursively discovers each single-layer GeoPackage and maps its Kart layer name
to the corresponding LDS shapefile through `core/layers_info.csv`.

Default paths are:

```text
Master: C:\Data\Topo50\kart-source\Release66_NZ50
Source: C:\Temp\export\lds_model
```

Run a comparison with common noise suppressed:

```powershell
uv run python utils\export\lds-convert\match_exports.py --skip-source-id-missing --first-value-mismatch-only
```

The comparator:

- Matches records by `t50_fid`, not row position.
- Compares all rows when the master layer has fewer than 101 rows.
- Otherwise compares a deterministic 10% sample.
- Logs missing/extra layers and fields, duplicate IDs, missing IDs, read errors,
  and field value mismatches.
- Records start, end, and elapsed time.

`--first-value-mismatch-only` logs only the first occurrence of the same
layer/field/master-value/source-value mismatch pattern. All occurrences remain
included in mismatch totals.

The comparison log is:

```text
C:\Temp\export\lds_model\match_exports.log
```

Show all runtime comparison options:

```powershell
uv run python utils\export\lds-convert\match_exports.py --help
```
