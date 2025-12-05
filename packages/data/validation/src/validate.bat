@echo off
REM Batch script for common topology validation scenarios
REM Usage: validate.bat [scenario] [additional_args...]

setlocal enabledelayedexpansion

if "%1"=="" (
    echo Usage: validate.bat [scenario] [additional_args...]
    echo.
    echo Available scenarios:
    echo   local-gpkg [gpkg_file]        - Validate local GeoPackage file
    echo   local-parquet [parquet_path]  - Validate local Parquet files
    echo   postgis [connection_string]   - Validate PostGIS database
    echo   quick-test                    - Quick test with sample data
    echo   help                          - Show detailed help
    echo.
    echo Examples:
    echo   validate.bat local-gpkg "C:\data\my_data.gpkg"
    echo   validate.bat postgis "postgresql://user:pass@localhost/mydb"
    echo   validate.bat local-parquet "C:\data\parquet_files"
    goto :eof
)

set SCENARIO=%1
shift

REM Set Python command (adjust if needed)
set PYTHON_CMD=python

REM Base validation script
set VALIDATION_SCRIPT=run_validation_cli.py

if "%SCENARIO%"=="local-gpkg" (
    if "%1"=="" (
        echo Error: GeoPackage file path required
        echo Usage: validate.bat local-gpkg [gpkg_file]
        goto :eof
    )
    echo Running validation on GeoPackage: %1
    %PYTHON_CMD% %VALIDATION_SCRIPT% --mode generic --db-url "%1" --verbose %2 %3 %4 %5 %6 %7 %8 %9
    goto :eof
)

if "%SCENARIO%"=="local-parquet" (
    if "%1"=="" (
        echo Error: Parquet file/directory path required
        echo Usage: validate.bat local-parquet [parquet_path]
        goto :eof
    )
    echo Running validation on Parquet files: %1
    %PYTHON_CMD% %VALIDATION_SCRIPT% --mode generic --db-url "%1" --verbose %2 %3 %4 %5 %6 %7 %8 %9
    goto :eof
)

if "%SCENARIO%"=="postgis" (
    if "%1"=="" (
        echo Error: PostgreSQL connection string required
        echo Usage: validate.bat postgis [connection_string]
        goto :eof
    )
    echo Running validation on PostGIS database: %1
    %PYTHON_CMD% %VALIDATION_SCRIPT% --mode postgis --db-url "%1" --verbose %2 %3 %4 %5 %6 %7 %8 %9
    goto :eof
)

if "%SCENARIO%"=="quick-test" (
    echo Running quick validation test...
    REM Adjust the path to your test data
    %PYTHON_CMD% %VALIDATION_SCRIPT% --mode generic --db-url "test_data.gpkg" --verbose --use-date-folder
    goto :eof
)

if "%SCENARIO%"=="help" (
    echo Topology Validation Tool - Detailed Help
    echo.
    echo This batch script provides convenient shortcuts for common validation scenarios.
    echo For more advanced options, use run_validation_cli.py directly.
    echo.
    echo Available scenarios:
    echo.
    echo 1. local-gpkg [gpkg_file] [options...]
    echo    - Validates a local GeoPackage file
    echo    - Example: validate.bat local-gpkg "C:\data\topo50.gpkg"
    echo    - Example: validate.bat local-gpkg "data.gpkg" --bbox 174.8 -41.3 174.9 -41.2
    echo.
    echo 2. local-parquet [parquet_path] [options...]
    echo    - Validates Parquet files in a directory or single file
    echo    - Example: validate.bat local-parquet "C:\data\parquet_files"
    echo    - Example: validate.bat local-parquet "data.parquet" --export-parquet
    echo.
    echo 3. postgis [connection_string] [options...]
    echo    - Validates data in a PostGIS database
    echo    - Example: validate.bat postgis "postgresql://user:pass@localhost/topo50"
    echo    - Example: validate.bat postgis "postgresql://user@localhost/db" --date today
    echo.
    echo 4. quick-test
    echo    - Runs a quick validation test (requires test_data.gpkg)
    echo.
    echo Common additional options you can add to any scenario:
    echo   --bbox [minx miny maxx maxy]  : Spatial bounding box filter
    echo   --date [YYYY-MM-DD^|today]     : Date filter
    echo   --weeks [number]              : Filter by weeks back
    echo   --output-dir [path]           : Custom output directory
    echo   --export-parquet              : Export results to Parquet format
    echo   --skip-queries                : Skip query-based validations
    echo   --skip-self-intersections     : Skip self-intersection checks
    echo.
    echo For complete options, run:
    echo   python run_validation_cli.py --help
    goto :eof
)

echo Unknown scenario: %SCENARIO%
echo Run "validate.bat help" for available scenarios