#!/usr/bin/env python3
"""
Command line interface for topology validation.
This script provides a CLI interface to run various topology validation checks
on geospatial datasets (PostGIS, GeoPackage, or Parquet files).
"""

import argparse
import sys
import os
from datetime import datetime
from topology_validator_tools import TopoValidatorSettings
from validate_dataset_controller import ValidateDatasetController


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Run topology validation on geospatial datasets',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run validation on PostGIS database
  python run_validation_cli.py --mode postgis --db-path "postgresql://user:pass@localhost/db"
  
  # Run validation on GeoPackage file
  python run_validation_cli.py --mode generic --db-path "data.gpkg"
  
  # Run validation on Parquet files with custom output
  python run_validation_cli.py --mode generic --db-path "data.parquet" --output-dir "/tmp/validation"
  
  # Run with bounding box and date filtering
  python run_validation_cli.py --mode generic --db-path "data.gpkg" --bbox 174.81 -41.31 174.82 -41.30 --date today
        """)
    
    # Required arguments
    parser.add_argument('--mode', 
                       choices=['generic', 'postgis'], 
                       default='generic',
                       help='Validation mode: generic for GPKG/Parquet, postgis for PostgreSQL/PostGIS')
    
    parser.add_argument('--db-path', 
                       required=True,
                       help='file path (GPKG/Parquet), Database URL (PostgreSQL connection string) ')
    
    parser.add_argument('--config-file', 
                       help='Path to validation configuration JSON file')
    
    parser.add_argument('--output-dir', 
                       required=True,
                       help='Output directory for validation results')
    
    parser.add_argument('--area-crs', 
                       type=int, 
                       default=2193,
                       help='CRS code for area calculations (default: 2193)')
    
    # Export format options
    parser.add_argument('--export-parquet', 
                       action='store_false',
                       default=False,
                       help='Export results to Parquet format')
    
    parser.add_argument('--export-parquet-by-geometry', 
                       action='store_false',
                       default=False,
                       help='Export Parquet files separated by geometry type')
    
    parser.add_argument('--export-gpkg', 
                       action='store_true', 
                       default=True,
                       help='Export results to GeoPackage format (default: True)')
    
    parser.add_argument('--no-export-gpkg', 
                       action='store_false', 
                       dest='export_gpkg',
                       help='Disable GeoPackage export')
    
    # Processing options
    parser.add_argument('--use-date-folder', 
                       action='store_true',
                       help='Create date-based subfolder in output directory')
        
    parser.add_argument('--report-only', 
                       action='store_true',
                       default=False,
                       help="Don't export validation data - other just report is created")
    
    parser.add_argument('--skip-queries', 
                       action='store_false', 
                       dest='process_queries',
                       help='Skip query-based validations')
    
    parser.add_argument('--skip-features-on-layer', 
                       action='store_false', 
                       dest='process_features_on_layer',
                       help='Skip features-on-layer validations')
    
    parser.add_argument('--skip-self-intersections', 
                       action='store_false', 
                       dest='process_self_intersections',
                       help='Skip self-intersection validations')
    
    # Spatial and temporal filtering
    parser.add_argument('--bbox', 
                       nargs=4, 
                       type=float, 
                       metavar=('MINX', 'MINY', 'MAXX', 'MAXY'),
                       help='Bounding box for spatial filtering (minx miny maxx maxy)')
    
    parser.add_argument('--date', 
                       help='Date for filtering (YYYY-MM-DD or "today")')
    
    parser.add_argument('--weeks', 
                       type=int, 
                       help='Number of weeks back for date filtering')
    
    # Verbose output
    parser.add_argument('-v', '--verbose', 
                       action='store_true',
                       help='Enable verbose output')
    
    return parser.parse_args()


def validate_arguments(args):
    """Validate command line arguments."""
    errors = []
    
    # Validate database URL/file path
    if args.mode == 'postgis':
        if not args.db_path.startswith('postgresql://'):
            errors.append("PostGIS mode requires a PostgreSQL connection string starting with 'postgresql://'")
    else:
        if not (args.db_path.endswith('.gpkg') or args.db_path.endswith('.parquet') or 'parquet' in args.db_path):
            errors.append("Generic mode requires a GPKG file (.gpkg) or Parquet file/directory (.parquet)")
        if not os.path.exists(args.db_path.replace('files.parquet', '')):
            errors.append(f"Database file/directory not found: {args.db_path}")
    
    # Validate config file if provided
    if args.config_file and not os.path.exists(args.config_file):
        errors.append(f"Configuration file not found: {args.config_file}")
    
    # Validate bounding box
    if args.bbox and len(args.bbox) != 4:
        errors.append("Bounding box must have exactly 4 values: minx miny maxx maxy")
    
    # Validate date format
    if args.date and args.date != 'today':
        try:
            datetime.strptime(args.date, '%Y-%m-%d')
        except ValueError:
            errors.append("Date must be in YYYY-MM-DD format or 'today'")
    
    return errors


def setup_settings(args):
    """Setup TopoValidatorSettings based on command line arguments."""
    settings = TopoValidatorSettings()
    
    # Set configuration file based on mode or user input
    if args.config_file:
        settings.validation_config_file = args.config_file
    elif args.mode == 'postgis':
        settings.validation_config_file = './validation_postgis_config.json'
    else:
        settings.validation_config_file = './validation_generic_config.json'
    
    # Database connection
    settings.db_path = args.db_path
    print(f"Using database path: {settings.db_path}")

    # Output settings
    settings.output_dir = args.output_dir
    settings.area_crs = args.area_crs
    if args.report_only:
        settings.export_validation_data = False
    
    # Export format settings
    settings.export_parquet = args.export_parquet
    settings.export_parquet_by_geometry_type = args.export_parquet_by_geometry
    settings.export_gpkg = args.export_gpkg
    
    # Processing settings
    settings.use_date_folder = args.use_date_folder
    settings.process_queries = args.process_queries
    settings.process_features_on_layer = args.process_features_on_layer
    settings.process_self_intersections = args.process_self_intersections
    
    # Filtering settings
    if args.bbox:
        settings.bbox = tuple(args.bbox)
    
    if args.date:
        settings.date = args.date
    
    if args.weeks:
        settings.weeks = args.weeks
    
    return settings


def main():
    """Main entry point for the CLI."""
    args = parse_arguments()
    
    # Validate arguments
    errors = validate_arguments(args)
    if errors:
        print("Error(s) in command line arguments:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        sys.exit(1)
    
    # Setup settings
    try:
        settings = setup_settings(args)
    except Exception as e:
        print(f"Error setting up validation settings: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Verbose output
    if args.verbose:
        print(f"Validation mode: {args.mode}")
        print(f"Database path: {settings.db_path}")
        print(f"Config file: {settings.validation_config_file}")
        print(f"Output directory: {settings.output_dir}")
        print(f"Export validation data: {settings.export_validation_data}")
        print(f"Export formats: GPKG={settings.export_gpkg}, Parquet={settings.export_parquet}")
        print(f"Use date folder: {settings.use_date_folder}")
        if hasattr(settings, 'bbox') and settings.bbox:
            print(f"Bounding box: {settings.bbox}")
        if hasattr(settings, 'date') and settings.date:
            print(f"Date filter: {settings.date}")
        if hasattr(settings, 'weeks') and settings.weeks:
            print(f"Weeks filter: {settings.weeks}")
    
    # Run validation
    try:
        print("Starting topology validation...")
        controller = ValidateDatasetController(settings)
        controller.run_validation()
        print("Validation completed successfully!")
        
    except KeyboardInterrupt:
        print("\nValidation interrupted by user", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"Error during validation: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()