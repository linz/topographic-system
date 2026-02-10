# Validate Topographic Datasets

This package provides topology validation for geospatial datasets including GeoPackage, Parquet, and PostGIS sources.

## Installation

```bash
cd packages/validation
uv sync
```

## Quick Start

```bash
# Validate a GeoPackage file
topographic_validation --mode generic --db-path "data.gpkg" --output-dir "./output"

# Validate PostGIS database
topographic_validation --mode postgis --db-path "postgresql://user:pass@localhost/db" --output-dir "./output"

# Validate with spatial filtering
topographic_validation --mode generic --db-path "data.gpkg" --output-dir "./output" --bbox 174.8 -41.3 174.9 -41.2
```

## Supported Data Sources

- **GeoPackage** (SQLite-based `.gpkg` files)
- **Parquet** (Apache Parquet files)
- **PostGIS** (PostgreSQL with PostGIS extension)

## Command Line Options

### Required Arguments

| Option | Description | Example |
|--------|-------------|---------|
| `--mode` | Validation mode: `postgis` or `generic` (default: `generic`) | `--mode generic` |
| `--db-path` | Database URL or file path | `--db-path "data.gpkg"` |
| `--output-dir` | Output directory for validation results | `--output-dir "./output"` |

### Optional Configuration

| Option | Description | Default |
|--------|-------------|---------|
| `--config-file` | Custom validation config JSON | Auto-selected based on mode |
| `--area-crs` | CRS for area calculations | `2193` |

### Export Formats

| Option | Description |
|--------|-------------|
| `--export-parquet` | Export to Parquet format |
| `--export-parquet-by-geometry` | Separate Parquet by geometry type |
| `--no-export-gpkg` | Disable GeoPackage export (enabled by default) |

### Processing Options

| Option | Description |
|--------|-------------|
| `--use-date-folder` | Create date-based output subfolders |
| `--report-only` | Don't export validation data - only create report |
| `--skip-queries` | Skip query-based validations |
| `--skip-features-on-layer` | Skip features-on-layer checks |
| `--skip-self-intersections` | Skip self-intersection checks |

### Filtering Options

| Option | Arguments | Description |
|--------|-----------|-------------|
| `--bbox` | minx miny maxx maxy | Spatial bounding box filter |
| `--date` | YYYY-MM-DD or "today" | Date filter |
| `--weeks` | number | Filter by weeks back |

### Other Options

| Option | Description |
|--------|-------------|
| `-v, --verbose` | Enable detailed output |
| `--help` | Show complete help |

## Examples

### Basic Validation

```bash
topographic_validation --mode generic --db-path "topo50.gpkg" --output-dir "./validation-output"
```

### PostGIS with Custom Output

```bash
topographic_validation \
    --mode postgis \
    --db-path "postgresql://user:pass@localhost/topo50" \
    --output-dir "/custom/output/path" \
    --verbose
```

### Spatial and Temporal Filtering

```bash
topographic_validation \
    --mode generic \
    --db-path "data.gpkg" \
    --output-dir "./output" \
    --bbox 174.81 -41.31 174.82 -41.30 \
    --date "2024-01-15" \
    --use-date-folder
```

### Custom Export Options

```bash
topographic_validation \
    --mode generic \
    --db-path "data.parquet" \
    --output-dir "./output" \
    --export-parquet \
    --export-parquet-by-geometry \
    --skip-queries \
    --verbose
```

### Report Only Mode

```bash
topographic_validation \
    --mode generic \
    --db-path "data.gpkg" \
    --output-dir "./output" \
    --report-only
```

## Validation Types

### Feature Intersection Checks

| Check Type | Description |
|------------|-------------|
| `feature_not_on_layers` | Point/line features must intersect specified layer |
| `feature_in_layers` | Features must not fall within specified layer |
| `line_not_on_feature_layers` | Line features must lie on specified layer |
| `line_not_touches_feature_layers` | Line features must not touch specified layer |
| `feature_not_contains_layers` | Polygon features must contain specified layer |

### Self-Intersection Checks

| Check Type | Description |
|------------|-------------|
| `self_intersect_layers` | Features must not self-intersect |

### Attribute Checks

| Check Type | Description |
|------------|-------------|
| `null_columns` | Specified columns must not be null |
| `query_rules` | Features must pass specified query rules |

## Configuration

Validation rules are defined in JSON configuration files. A default configuration is provided at `config/default_config.json`.

The CLI automatically selects configuration files based on mode:
- **PostGIS mode**: `./validation_postgis_config.json`
- **Generic mode**: `./validation_generic_config.json`
- **Custom**: Use `--config-file` option

### Configuration Structure

```json
{
  "feature_not_on_layers": [...],
  "feature_in_layers": [...],
  "line_not_on_feature_layers": [...],
  "line_not_touches_feature_layers": [...],
  "feature_not_contains_layers": [...],
  "self_intersect_layers": [...],
  "null_columns": [...],
  "query_rules": [...]
}
```

### Feature Intersection Configuration

```json
{
  "table": "building_point",
  "intersection_table": "building",
  "layername": "building-points-in-building-polygons",
  "message": "Building point features must not fall within building polygon features"
}
```

Optional filters:
- `"where": "feature_type = 'value'"` - SQL where clause filter
- `"date": "today"` or `"date": "2025-10-01"` - Filter by update date
- `"weeks": 1` - Filter by changes in last N weeks

### Self-Intersection Configuration

```json
{
  "table": "vegetation",
  "layername": "vegetation-validation",
  "message": "Vegetation features must not self-intersect"
}
```

### Null Column Configuration

```json
{
  "table": "descriptive_text",
  "column": "info_display",
  "message": "Descriptive text features must have an info_display attribute",
  "where": "(feature_type = 'waterfall' OR feature_type = 'soakhole')"
}
```

### Query Rule Configuration

```json
{
  "table": "vegetation",
  "column": "species",
  "where": "feature_type = 'exotic'",
  "rule": "species IN ('coniferous', 'non-coniferous')",
  "message": "Exotic vegetation must have species as coniferous or non-coniferous"
}
```

## Output

Validation results are saved to the output directory:

```
output-dir/
├── [date-folder]/              # If --use-date-folder enabled
├── validation_results.gpkg     # GeoPackage export (default)
├── *.parquet                   # Parquet exports (if enabled)
└── validation_summary_report.json  # Summary report
```

## Architecture

### Core Classes

- **`TopoValidatorSettings`** - Configuration and settings management for validation runs
- **`ValidateDatasetController`** - Main controller that orchestrates validation execution
- **`TopologyValidatorFactory`** - Factory for creating appropriate validators based on data source
- **`TopoValidatorTools`** - Utilities for folder management and date formatting

### Validators

Located in `src/topographic_validation/validators/`:

- **`GpkgValidator`** - Validates GeoPackage files
- **`ParquetValidator`** - Validates Parquet files
- **`PostgisValidator`** - Validates PostGIS databases

## Troubleshooting

### Common Issues

1. **"Database file not found"**
   - Check file path is correct and accessible
   - Use absolute paths for reliability

2. **"PostgreSQL connection failed"**
   - Verify connection string format: `postgresql://user:pass@host:port/database`
   - Check database server is running and accessible

3. **"Configuration file not found"**
   - Ensure config file exists in specified path
   - Use `--config-file` with absolute path

4. **"Permission denied on output directory"**
   - Check write permissions for output directory

### Debug Mode

Use `--verbose` flag for detailed logging:

```bash
topographic_validation --mode generic --db-path "data.gpkg" --output-dir "./output" --verbose
```
