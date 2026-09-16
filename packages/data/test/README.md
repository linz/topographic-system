# README_TEST: Data Package Test Coverage

This document describes the pytest test suites for the data package import and management scripts.

## postgis_create_model.py test notes

These tests validate the script behavior without requiring a real Excel file or a running Postgres instance.

### 1. Test setup helpers

- `_fake_dataframe` builds a synthetic DataFrame that mimics Excel input for one layer and several field types.
- `_run_script_and_capture_sql` executes the script as `__main__` and records SQL statements sent to a fake cursor.
- `psycopg` is stubbed via `sys.modules` so tests do not require native `libpq` bindings.
- `pandas.read_excel` is monkeypatched to return the synthetic DataFrame.

### 2. Parsing test

`test_excel_to_layered_dict_groups_by_dataset_and_layer` verifies that:

- Dataset names have `_Layers` removed (for example, `Transport_Layers` -> `Transport`).
- Layer keys are grouped under the correct dataset (`Road_Line` under `Transport`).
- Field metadata is stored as expected (for example, `feature_type` with `type` and `length`).

### 3. Main SQL generation test

`test_main_generates_drop_and_create_sql` runs the script path and verifies SQL includes:

- `DROP TABLE IF EXISTS release64.road_line CASCADE;`
- A `CREATE TABLE` statement with expected mapped columns:
  - `topo_id uuid DEFAULT gen_random_uuid()`
  - `feature_type VARCHAR(50)` from `STRING`
  - `lane_count INTEGER` from `INTEGER`
  - `t50_fid BIGINT` from `BIGINTEGER`
  - `is_active BOOLEAN` from `BOOLEAN`
  - `geometry geometry(LINESTRING, 2193)` from Polyline geometry normalization

### 4. Field exclusion test

`test_main_skips_objectid_field_in_create_sql` verifies that `objectid` is excluded from generated `CREATE TABLE` SQL.

## load_shp_to_themes.py test notes

These tests validate the Topo50DataLoader class without requiring actual shapefiles or database connections.

### 1. Test fixtures

- `sample_layers_excel`: DataFrame with layer metadata (object_name, shp_name, theme, dataset, feature_type, layer_name).
- `sample_geodataframe`: GeoDataFrame with geometry and sample attribute columns.
- `temp_count_log`: Temporary log file for row count tracking.

### 2. Filename parsing tests

`test_get_basename_*` tests validate:

- Simple filenames (water.shp) return basename unchanged.
- Single-underscore filenames (road_line.shp) extract prefix up to underscore.
- Multi-underscore filenames extract first two underscore-separated tokens.

### 3. Loader initialization tests

`test_loader_initialization` verifies:

- Excel file is read and stored in `layers_info` mapping.
- Configuration parameters (database, dataset_field) are initialized.

`test_loader_layers_info_parsed_correctly` validates the nested structure:

- object_name, theme, feature_type, layer_name, and dataset are all stored.

### 4. Column renaming tests

`test_reset_column_names_tunnel_line` validates layer-specific rules:

- `use1` → `tunnel_use`
- `use2` → `tunnel_use2`
- `type` → `tunnel_type`

`test_reset_column_names_road_line` validates type conversions:

- `hway_num` → `highway_number`
- `num_lanes` → `lane_count` (converted to int, NaN → 0)
- `UFID` → `t50_fid` (converted to int, NaN → 0)

`test_reset_column_names_generic_abbreviations` tests universal abbreviations:

- `compositn` → `composition`
- `descriptn` → `description`
- `info_disp` → `info_display`
- `veh_type` → `vehicle_type`
- `temp` → `temperature`
- `restrictns` → `restrictions`
- `orientatn` → `orientation`

### 5. Layer grouping tests

`test_group_layers` verifies:

- Shapefiles are matched to layers using `shp_name` from layers_info.
- Field definitions are collected per layer in `layer_groups`.

### 6. Field harmonization test

`test_compute_common_fields` validates:

- Union of all fields across multiple shapefiles for the same layer.
- Result stored in `common_fields` mapping.

### 7. Output format tests

`test_write_dataset_postgis` mocks database write to PostGIS.

`test_write_dataset_geojson` validates GeoJSON write with correct driver.

`test_write_dataset_gpkg` validates GeoPackage write with layer name.

`test_write_dataset_handles_exceptions` verifies exceptions are caught and logged without raising.

## postgis_manage_fields.py test notes

These tests validate ModifyTable DDL/DML operations and TableModificationWorkflow orchestration without a real database.

### 1. Test setup

- `mock_psycopg` fixture patches psycopg module and provides mock connections/cursors.
- All database operations use mocked execute() calls for SQL validation.

### 2. ModifyTable initialization

`test_modify_table_initialization` verifies db_params are stored and connection established.

### 3. Schema inspection tests

`test_table_exists_returns_true_when_found` validates table existence check in information_schema.

`test_list_schema_tables_returns_grouped_tables` validates dict mapping of schema->tables.

`test_column_exists_returns_true_when_found` validates column existence queries.

`test_column_exists_with_like_pattern` validates pattern matching with LIKE operator.

`test_column_list_returns_matching_columns` returns all columns matching substring pattern.

### 4. Column management tests

`test_add_column_without_default` validates DEFAULT NULL is appended when not specified.

`test_add_column_with_default` validates explicit DEFAULT is preserved (no double DEFAULT).

`test_rename_columns_when_column_exists` executes ALTER TABLE RENAME when column found.

`test_rename_columns_skips_when_column_missing` skips rename for missing columns.

`test_drop_column_when_exists` executes DROP COLUMN for existing columns.

### 5. Column value updates

`test_update_column_with_default_without_where` updates all rows when no WHERE clause.

`test_update_column_with_default_with_where` applies WHERE predicate to UPDATE statement.

### 6. Geometry tests

`test_get_srid_returns_value` queries and returns SRID from Find_SRID function.

`test_get_srid_returns_none_when_not_found` returns None when column doesn't exist.

`test_get_geometry_type_returns_type` returns geometry type (POLYGON, LINESTRING, etc).

`test_carto_text_geom_update_snaps_to_grid` validates ST_SnapToGrid usage for 1m grid.

### 7. Primary key tests

`test_update_primary_key_creates_sequence` validates:

- Sequence creation with proper naming.
- DEFAULT nextval() assignment.
- PRIMARY KEY constraint added.

`test_update_primary_key_guid_uses_gen_random_uuid` validates:

- DEFAULT gen_random_uuid() for UUID columns.
- PRIMARY KEY constraint added.

### 8. Column ordering tests

`test_all_ordered_columns_default_keeps_id` validates int primary key keeps `id` column.

`test_all_ordered_columns_uuid_moves_topo_id_first` validates UUID primary key reorders with `topo_id` first, removes `id`.

### 9. TableModificationWorkflow tests

`test_table_modification_workflow_initialization` validates workflow configuration storage.

`test_table_modification_workflow_should_run_all` validates all steps run when option='all'.

`test_table_modification_workflow_should_run_specific_step` validates step filtering.

## Test Summary

| Module                   | Tests  | Status       |
| ------------------------ | ------ | ------------ |
| postgis_create_model.py  | 3      | ✓ passed     |
| load_shp_to_themes.py    | 14     | ✓ passed     |
| postgis_manage_fields.py | 25     | ✓ passed     |
| **Total**                | **42** | **✓ passed** |

## Files

**postgis_create_model.py:**

- Script: `packages/data/nztopo50/import/core/postgis_create_model.py`
- Tests: `packages/data/test/test_postgis_create_model.py`

**load_shp_to_themes.py:**

- Script: `packages/data/nztopo50/import/core/load_shp_to_themes.py`
- Tests: `packages/data/test/test_load_shp_to_themes.py`

**postgis_manage_fields.py:**

- Script: `packages/data/nztopo50/import/core/postgis_manage_fields.py`
- Tests: `packages/data/test/test_postgis_manage_fields.py`
