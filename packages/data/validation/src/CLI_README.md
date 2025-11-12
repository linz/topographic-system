# Topology Validation CLI Tools

This directory contains command-line tools for running topology validation on geospatial datasets.

## Files Overview

- **`run_validation_cli.py`** - Main command-line interface with full argument support
- **`run_validation.py`** - Original script with hardcoded settings (dveloper version)
- **`validate.bat`** - Windows batch script wrapper for common scenarios
- **`validate.ps1`** - PowerShell script wrapper with enhanced features

## Quick Start

### 1. Basic CLI Usage

```bash
# Validate a GeoPackage file
python run_validation_cli.py --mode generic --db-url "data.gpkg"

# Validate PostGIS database
python run_validation_cli.py --mode postgis --db-url "postgresql://user:pass@localhost/db"

# Validate with spatial filtering
python run_validation_cli.py --mode generic --db-url "data.gpkg" --bbox 174.8 -41.3 174.9 -41.2
```

### 2. Using Wrapper Scripts (Windows)

```cmd
REM Batch script usage
validate.bat local-gpkg "C:\data\topo50.gpkg"
validate.bat postgis "postgresql://user:pass@localhost/topo50"
validate.bat help
```

```powershell
# PowerShell script usage
.\validate.ps1 local-gpkg "C:\data\topo50.gpkg"  
.\validate.ps1 local-parquet "C:\data\parquet_files"
.\validate.ps1 help
```

## Command Line Options

### Required Arguments

| Option | Description | Example |
|--------|-------------|---------|
| `--mode` | Validation mode: `postgis` or `generic` | `--mode generic` |
| `--db-url` | Database URL or file path | `--db-url "data.gpkg"` |

### Optional Configuration

| Option | Description | Default |
|--------|-------------|---------|
| `--config-file` | Custom validation config JSON | Auto-selected based on mode |
| `--output-dir` | Output directory | `C:\temp\validation-data` |
| `--area-crs` | CRS for area calculations | `2193` |

### Export Formats

| Option | Description |
|--------|-------------|
| `--export-parquet` | Export to Parquet format |
| `--export-parquet-by-geometry` | Separate Parquet by geometry type |
| `--export-gpkg` | Export to GeoPackage (default: True) |
| `--no-export-gpkg` | Disable GeoPackage export |

### Processing Options

| Option | Description |
|--------|-------------|
| `--use-date-folder` | Create date-based output subfolders |
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
| `--verbose` | Enable detailed output |
| `--help` | Show complete help |

## Examples

### 1. Basic Validation

```bash
# Validate GeoPackage with default settings
python run_validation_cli.py --mode generic --db-url "topo50.gpkg"
```

### 2. PostGIS with Custom Output

```bash
# Validate PostGIS database with custom output directory
python run_validation_cli.py \
    --mode postgis \
    --db-url "postgresql://user:pass@localhost/topo50" \
    --output-dir "/custom/output/path" \
    --verbose
```

### 3. Spatial and Temporal Filtering

```bash
# Validate with bounding box and date filtering
python run_validation_cli.py \
    --mode generic \
    --db-url "data.gpkg" \
    --bbox 174.81 -41.31 174.82 -41.30 \
    --date "2024-01-15" \
    --use-date-folder
```

### 4. Custom Export Options

```bash
# Export to both Parquet and GeoPackage, skip some validations
python run_validation_cli.py \
    --mode generic \
    --db-url "data.parquet" \
    --export-parquet \
    --export-parquet-by-geometry \
    --skip-queries \
    --verbose
```

### 5. Using Configuration Files

```bash
# Use custom validation configuration
python run_validation_cli.py \
    --mode generic \
    --db-url "data.gpkg" \
    --config-file "custom_validation_config.json"
```

## Wrapper Script Scenarios

The batch and PowerShell wrapper scripts provide convenient presets:

### Available Scenarios

1. **`local-gpkg`** - Validate local GeoPackage files
2. **`local-parquet`** - Validate local Parquet files/directories  
3. **`postgis`** - Validate PostGIS database
4. **`quick-test`** - Run quick test validation
5. **`help`** - Show detailed help

### Batch Script Examples

```cmd
REM Basic validation
validate.bat local-gpkg "C:\data\topo50.gpkg"

REM With additional options
validate.bat local-gpkg "data.gpkg" --bbox 174.8 -41.3 174.9 -41.2 --export-parquet

REM PostGIS validation
validate.bat postgis "postgresql://user:pass@localhost/topo50" --date today
```

### PowerShell Script Examples

```powershell
# Basic validation with enhanced error checking
.\validate.ps1 local-gpkg "C:\data\topo50.gpkg"

# Parquet validation with export options
.\validate.ps1 local-parquet "C:\data\parquet" --export-parquet --use-date-folder

# PostGIS validation with filtering
.\validate.ps1 postgis "postgresql://user@localhost/db" --weeks 2 --verbose
```

## Configuration Files

The CLI automatically selects appropriate configuration files:

- **PostGIS mode**: `validation_postgis_config.json`
- **Generic mode**: `validation_generic_config.json`
- **Custom**: Use `--config-file` option

## Output Structure

Validation results are saved to the output directory with this structure:

```
output-dir/
├── [date-folder]/          # If --use-date-folder enabled
├── validation_results.gpkg # GeoPackage export (default)
├── *.parquet              # Parquet exports (if enabled)
├── logs/                  # Validation logs
└── reports/               # Summary reports
```

## Error Handling

The CLI provides helpful error messages for:

- Invalid file paths or database connections
- Incorrect argument formats
- Missing configuration files
- Processing errors during validation

Use `--verbose` flag for detailed error information and progress tracking.

## Migration from Legacy Script

To migrate from `run_validation.py`:

1. Replace hardcoded settings with command-line arguments
2. Use `--config-file` for custom validation rules
3. Set `--output-dir` to match your current output location
4. Add filtering options (`--bbox`, `--date`) as needed

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
   - Try using a different output directory with `--output-dir`

### Debug Mode

Use `--verbose` flag for detailed logging:

```bash
python run_validation_cli.py --mode generic --db-url "data.gpkg" --verbose
```

This enables detailed output showing:
- Configuration settings used
- Processing steps and progress
- File operations and exports
- Error details and stack traces