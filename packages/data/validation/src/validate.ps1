#!/usr/bin/env powershell
<#
.SYNOPSIS
    PowerShell wrapper for topology validation CLI
    
.DESCRIPTION
    Provides convenient shortcuts for common topology validation scenarios.
    This script wraps the Python CLI with user-friendly presets.
    
.PARAMETER Scenario
    The validation scenario to run (local-gpkg, local-parquet, postgis, quick-test, help)
    
.PARAMETER Path
    Path to the data file or database connection string
    
.PARAMETER AdditionalArgs
    Additional arguments to pass to the validation script
    
.EXAMPLE
    .\validate.ps1 local-gpkg "C:\data\topo50.gpkg"
    
.EXAMPLE
    .\validate.ps1 postgis "postgresql://user:pass@localhost/topo50" -bbox 174.8,-41.3,174.9,-41.2
    
.EXAMPLE
    .\validate.ps1 local-parquet "C:\data\parquet" -ExportParquet -DateToday
#>

param(
    [Parameter(Mandatory=$true, Position=0)]
    [ValidateSet("local-gpkg", "local-parquet", "postgis", "quick-test", "help")]
    [string]$Scenario,
    
    [Parameter(Position=1)]
    [string]$Path,
    
    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$AdditionalArgs
)

# Configuration
$PythonCmd = "python"
$ValidationScript = "run_validation_cli.py"

# Helper function to run validation
function Invoke-Validation {
    param(
        [string]$Mode,
        [string]$DbUrl,
        [string[]]$ExtraArgs = @()
    )
    
    $args = @(
        $ValidationScript,
        "--mode", $Mode,
        "--db-url", $DbUrl,
        "--verbose"
    )
    
    if ($ExtraArgs.Count -gt 0) {
        $args += $ExtraArgs
    }
    
    Write-Host "Running: $PythonCmd $($args -join ' ')" -ForegroundColor Green
    & $PythonCmd @args
}

# Process scenarios
switch ($Scenario) {
    "local-gpkg" {
        if (-not $Path) {
            Write-Error "GeoPackage file path required for local-gpkg scenario"
            Write-Host "Usage: .\validate.ps1 local-gpkg 'C:\path\to\file.gpkg'" -ForegroundColor Yellow
            exit 1
        }
        
        if (-not (Test-Path $Path)) {
            Write-Error "GeoPackage file not found: $Path"
            exit 1
        }
        
        Write-Host "Validating GeoPackage: $Path" -ForegroundColor Cyan
        Invoke-Validation -Mode "generic" -DbUrl $Path -ExtraArgs $AdditionalArgs
    }
    
    "local-parquet" {
        if (-not $Path) {
            Write-Error "Parquet file/directory path required for local-parquet scenario"
            Write-Host "Usage: .\validate.ps1 local-parquet 'C:\path\to\parquet'" -ForegroundColor Yellow
            exit 1
        }
        
        if (-not (Test-Path $Path)) {
            Write-Error "Parquet file/directory not found: $Path"
            exit 1
        }
        
        Write-Host "Validating Parquet files: $Path" -ForegroundColor Cyan
        Invoke-Validation -Mode "generic" -DbUrl $Path -ExtraArgs $AdditionalArgs
    }
    
    "postgis" {
        if (-not $Path) {
            Write-Error "PostgreSQL connection string required for postgis scenario"
            Write-Host "Usage: .\validate.ps1 postgis 'postgresql://user:pass@localhost/db'" -ForegroundColor Yellow
            exit 1
        }
        
        if (-not $Path.StartsWith("postgresql://")) {
            Write-Error "Connection string must start with 'postgresql://'"
            exit 1
        }
        
        Write-Host "Validating PostGIS database: $Path" -ForegroundColor Cyan
        Invoke-Validation -Mode "postgis" -DbUrl $Path -ExtraArgs $AdditionalArgs
    }
    
    "quick-test" {
        Write-Host "Running quick validation test..." -ForegroundColor Cyan
        $testFile = "test_data.gpkg"
        
        if (-not (Test-Path $testFile)) {
            Write-Warning "Test file '$testFile' not found. Creating placeholder validation..."
            $testFile = "dummy_test.gpkg"
        }
        
        Invoke-Validation -Mode "generic" -DbUrl $testFile -ExtraArgs @("--use-date-folder")
    }
    
    "help" {
        Write-Host @"

Topology Validation Tool - PowerShell Wrapper
============================================

Available Scenarios:
------------------

1. local-gpkg <file_path> [options...]
   Validates a local GeoPackage file
   
   Examples:
   .\validate.ps1 local-gpkg "C:\data\topo50.gpkg"
   .\validate.ps1 local-gpkg "data.gpkg" --bbox 174.8 -41.3 174.9 -41.2

2. local-parquet <directory_path> [options...]
   Validates Parquet files in a directory or single file
   
   Examples:
   .\validate.ps1 local-parquet "C:\data\parquet_files"
   .\validate.ps1 local-parquet "data.parquet" --export-parquet

3. postgis <connection_string> [options...]
   Validates data in a PostGIS database
   
   Examples:
   .\validate.ps1 postgis "postgresql://user:pass@localhost/topo50"
   .\validate.ps1 postgis "postgresql://user@localhost/db" --date today

4. quick-test
   Runs a quick validation test (requires test_data.gpkg)

Common Additional Options:
------------------------
  --bbox <minx> <miny> <maxx> <maxy>  : Spatial bounding box filter
  --date <YYYY-MM-DD|today>           : Date filter  
  --weeks <number>                    : Filter by weeks back
  --output-dir <path>                 : Custom output directory
  --export-parquet                    : Export results to Parquet format
  --skip-queries                      : Skip query-based validations
  --skip-self-intersections           : Skip self-intersection checks

PowerShell-Specific Features:
---------------------------
  - Tab completion for scenario names
  - Path validation and error checking
  - Colored output for better readability
  - Automatic quoting of file paths with spaces

For complete CLI options, run:
  python run_validation_cli.py --help

"@ -ForegroundColor White
    }
}

# Check exit code and provide helpful message
if ($LASTEXITCODE -ne 0 -and $Scenario -ne "help") {
    Write-Host "`nValidation completed with errors (exit code: $LASTEXITCODE)" -ForegroundColor Red
    Write-Host "Run '.\validate.ps1 help' for usage information" -ForegroundColor Yellow
}