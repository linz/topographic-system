# NZTOPO50 Data Processing Workflow

## Overview
This document describes the comprehensive data processing workflow implemented in `postgis_manage_fields.py` for transforming topographic data from source shapefiles to the standardized topographic model.

## Data Transformations

### 1. Coordinate System Conversions

#### Primary Conversion
- **Target CRS**: EPSG:4167 (New Zealand Geodetic Datum 2000 - NZGD2000)
- **Type**: Geographic (lat/long) coordinate system based on GRS80 ellipsoid
- **Scope**: All data going into topographic-data repository

#### Exceptions - Retained Original Projections
- **nz_topo50_map_sheet**: NZTM2000 (EPSG:2193)
- **Carto_text**: NZTM2000 (EPSG:2193) 
- **grid 1**: WGS84
- **grid 2**: NZTM2000 (EPSG:2193)

### 2. Field Management

#### Shapefile Field Name Restrictions
- **Issue**: Shapefile field names are restricted in length (10 characters)
- **Solution**: Renamed to longer, more descriptive versions

#### Standard Field Additions
- **feature_type**: Added to all layers based on shapefile name
- **topo_id**: UUID/GUID field added with unique value assignment
- **Metadata fields**: Comprehensive metadata added to all layers

#### Field Renaming Patterns
Systematic renaming of `use` and `type` fields based on layer name:

```python
update_dict = {
    (schema_name, "structure_point"): [
        ("structure_use", "shaft_use"),
        ("structure_type", "tank_type"),
        ("material", "materials"),
    ],
    (schema_name, "structure_line"): [("status", "dam_status")],
    (schema_name, "structure"): [
        ("species", "species_cultivated"),
        ("lid_type", "reservoir_lid_type"),
        ("structure_type", "tank_type"),
    ],
    (schema_name, "road_line"): [("highway_number", "highway_numb")],
    (schema_name, "water"): [
        ("water_use", "pond_use"),
        ("height", "elevation"),
    ],
}
```

### 3. Data Cleanup

#### Field Removal
- **ESRI_OID**: Dropped from all shapefiles where present
- **tree_locations name field**: Dropped (only 3 trees had names in source, kept for LDS recreation process record)

#### Column Reordering
- **Geometry field**: Moved to last column position
- **Standard ordering**: Applied consistent column ordering across all tables

### 4. Specialized Processing

#### Island Classification
- **Pre-processing step**: Islands intersected with created sea polygon (coastline + outer box)
- **New field**: `location` where:
  - `1` = sea-based island
  - `0` = inland island

#### Road Line Enhancements
- **Future field**: Added for upcoming functionality

### 5. Data Quality Corrections

#### Value Standardization
- **tunnel_line**: 'ivestock' → 'livestock'

#### Conditional Field Updates
- **tunnel_use**: Updated to 'vehicle' where `use2 = 'vehicle'`
- **tunnel_use2**: Updated to 'livestock' where `use2 = 'vehicle'`

#### Specific Value Corrections
- **trig_point**: `trig_type` set to 'beacon'
- **road_line way_count**: Set to 'one way' where `way_count = '1'`
- **road_line road_access**: 'mp' → 'm' where `road_access = 'm'`
- **physical_infrastructure_line**: `support_type` set to 'pole' where `feature_type = 'telephone'`

### 6. Name Field Additions

Name fields added to the following layers:
- physical_infrastructure_point
- physical_infrastructure_line
- structure
- vegetation
- landcover
- landcover_line
- ferry_crossing

### 7. Default Value Assignment

#### Null Value Replacements
Where fields are null, default values are assigned:

```python
update_dict = {
    f"{schema_name}.runway": [("surface", "'grass'", "")],
    f"{schema_name}.vegetation": [
        ("species", "'coniferous'", "AND feature_type = 'exotic'")
    ],
    f"{schema_name}.railway_line": [("vehicle_type", "'train'", "")],
}
```

### 8. Metadata Fields

#### Standard Metadata Schema
```python
fieldList = [
    ["capture_method", "VARCHAR(25) DEFAULT 'manual'", "DEFAULT"],
    ["change_type", "VARCHAR(25) DEFAULT 'new'", "DEFAULT"],
    ["update_date", "DATE DEFAULT CURRENT_DATE", "DEFAULT"],
    ["topo_id", "uuid DEFAULT gen_random_uuid()", "DEFAULT"],
    ["create_date", "DATE DEFAULT CURRENT_DATE", "DEFAULT"],
    ["version", "INTEGER DEFAULT 1", "DEFAULT"],
]
```

## Implementation Methods

### Primary Processing Functions

1. **`add_metadata_columns()`**: Adds standard metadata fields
2. **`populate_defined_null_values()`**: Sets default values for null fields
3. **`set_default_values()`**: Configures column default values
4. **`recreate_table_srid()`**: Handles coordinate system transformations
5. **`rename_columns()`**: Manages field name changes
6. **`add_name_columns()`**: Adds name fields to specified layers

### Database Operations

- **Primary Key Management**: UUID-based primary keys with sequence support
- **Index Creation**: Spatial (GIST) and attribute indexes
- **Column Management**: Add, rename, drop operations
- **Value Updates**: Conditional and bulk update operations

## Data Quality Assurance

### Field Validation
- Column existence checks before operations
- Table existence validation
- Schema consistency verification

### Error Handling
- Comprehensive exception handling for all database operations
- Rollback capabilities for failed transactions
- Detailed logging of all operations

## Processing Workflow Summary

1. **Source Data Import**: Load shapefiles into PostGIS
2. **Field Standardization**: Apply naming conventions and add standard fields
3. **Coordinate Transformation**: Convert to target CRS (EPSG:4167)
4. **Data Quality**: Apply corrections and standardizations
5. **Metadata Enhancement**: Add comprehensive metadata fields
6. **Index Creation**: Create spatial and attribute indexes
7. **Validation**: Verify data integrity and completeness

This workflow ensures consistent, high-quality topographic data suitable for the LINZ topographic system while maintaining compatibility with existing LDS (LINZ Data Service) processes.