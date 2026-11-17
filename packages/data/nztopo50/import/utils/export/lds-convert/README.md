# LDS Conversion

Convert the consolidated NZ Topo50 model into the legacy LDS shapefile layout.

Run all commands from:

```text
packages/data/nztopo50/import
```

## Components

| Path | Purpose |
|---|---|
| `export_to_lds_model.py` | Reads model GeoParquet files and writes LDS shapefiles |
| `match_exports.py` | Compares sampled export attributes against latest Kart GeoPackages |
| `topo50_schemas/data_path.json` | Named local/cloud GeoParquet URLs and per-layer path conventions |
| `topo50_schemas/data_sources.json` | Source model layers available in each export extent |
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


## Configure GeoParquet Source

Configure named locations in `topo50_schemas/data_path.json`. Each location has
a base `url` and a `data_path` template containing `{layer_name}`. The selected
location must expose all topo, contour, and product layers.

```json
{
  "default": "local",
  "locations": {
    "local": {
      "url": "C:\\Data\\temp\\topographic-data-dev-67",
      "data_path": "{layer_name}.parquet"
    },
    "nztopodev": {
      "url": "https://d1jzh93b1t1cv.cloudfront.net/data",
      "data_path": "{layer_name}/latest/{layer_name}.parquet"
    }
  }
}
```

## Run the Export

The CLI accepts an extent and an optional configured location name. If the
location is omitted, the `default` from `data_path.json` is used.

```powershell
uv run python utils\export\lds-convert\export_cli.py new_zealand_mainland
uv run python utils\export\lds-convert\export_cli.py new_zealand_mainland nztopodev
```

The export is written below `C:\temp\export\lds_model`, with one folder per
configured export area. The run log is:

```text
<output_folder>/export_to_lds_model.log
```

Unreadable or missing source layers are logged and skipped so the remaining
outputs can continue.

## Export Processing

For each LDS target, the exporter:

1. Uses DuckDB Spatial to query the configured local or cloud GeoParquet layer.
2. Prunes rows with the GeoParquet `bbox` covering and applies an exact geometry intersection.
3. Filters by `type` in SQL when the source layer feeds multiple LDS targets.
4. Reconstructs legacy source fields from the current model.
5. Recreates macron and ASCII name fields where required.
6. Reorders and renames fields using the target config.
7. Builds the Fiona schema dynamically from `target_type` and `geometry_type`.
8. Converts model underscores back to spaces in schema-declared string fields.
9. Reprojects to EPSG:2193 and writes an UTF-8 shapefile.

Current special reverse mappings include:

- Road `rna_sufi` recovered from the `road_id` value in metadata.
- Defaulted runway surface `grass` restored to null.
- Tunnel use corrections reversed, including swapped `vehicle`/`livestock`,
  `foot_traffic` spacing, and the source spelling `ivestock`.
- Bivouac material `building` restored to null.
- Defaulted exotic species `coniferous` restored to null.
- Tree name exceptions retained for known `t50_fid` values.

## Compare an Export

`match_exports.py` uses the LDS direct export tree as the reference data. It
recursively discovers each single-layer GeoPackage and maps its Kart layer name
to the corresponding LDS shapefile through `core/layers_info.csv`.

Default paths are:

```text
LDS direct export: C:\Data\Topo50\kart-source\latest
New LDS shapefiles: C:\Temp\export\lds_model
```

These paths can be overridden with `--lds-direct-export-folder` and
`--new-lds-shapefiles-folder`.

Run a comparison with common noise suppressed:

```powershell
uv run python utils\export\lds-convert\match_exports.py --skip-shp-file-id-missing --first-value-mismatch-only
```

`snow_poly` comparison is skipped by default because the output is derived
from `ice_poly`. Use `--no-skip-snow-poly` to include it.

Compare every record in only the `road_cl` layer:

```powershell
uv run python utils\export\lds-convert\match_exports.py --layer road_cl --sample-fraction 1
```

`--layer` accepts the LDS shapefile name with or without the `.shp` suffix and
matches it case-insensitively.

The comparator:

- Matches records by `t50_fid`, not row position.
- Includes the GeoPackage native feature ID in the comparison columns when that
  column is named `t50_fid`.
- Compares all rows when the LDS direct export layer has fewer than 101 rows.
- Otherwise compares a deterministic 10% sample.
- Logs missing/extra layers and fields, duplicate IDs, missing IDs, read errors,
  and field value mismatches.
- Records start, end, and elapsed time.

`--first-value-mismatch-only` logs only the first occurrence of the same
layer/field/LDS-direct-export-value/shp-file-value mismatch pattern. All
occurrences remain included in mismatch totals.

The comparison log is:

```text
C:\Temp\export\lds_model\match_exports.log
```

Show all runtime comparison options:

```powershell
uv run python utils\export\lds-convert\match_exports.py --help
```
