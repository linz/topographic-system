## Data Processing Workflow (Current as implemented)

Last checked: 2026-06-17

This document reflects the current behaviour in:
- core/load_shp_to_themes.py
- core/postgis_manage_fields.py

## 1) Load and normalize source layers (load_shp_to_themes.py)

### Source selection
- The current run path uses `source_mode = "gpkg_files"` and targets PostGIS.
- Input files are grouped by logical layer using `layers_info.csv`.
- Both `shp_name` and `kart_layer_name` are accepted for mapping.

### Layer harmonization
- For each logical layer, field sets from all contributing files are unioned.
- Missing columns are added as `None` so all rows for a layer share one schema.
- `feature_type` is added from the mapping file (`type` column), then renamed to `type`.

### Projection and column order
- Data is transformed to EPSG:2193 before writing.
- Geometry is forced to be the last column.

### Key active field renames in loader
- Generic short-field expansions:
  - `compositn -> composition`
  - `descriptn -> description`
  - `info_disp -> info_display`
  - `veh_type -> vehicle_type`
  - `restrictns -> restrictions`
  - `orientatn -> orientation`
  - `constr_typ -> construction_type`
  - `support_typ -> support_type`
  - `support_ty -> support_type`
  - `bldg_use -> building_use`
  - `pipe_use -> utility_use`
  - `rway_use -> railway_use`
  - `embkmt_use -> relief_use`
  - `grp_ascii -> group_ascii`
  - `grp_macron -> group_macron`
  - `grp_name -> group_name`
  - `substance -> substance_extracted`

- Layer-specific examples:
  - `tunnel_line`: `use1 -> tunnel_use`, `use2 -> tunnel_use2`, `type -> subtype`
  - `structure`: `type -> subtype`
  - `structure_point`: `use -> structure_use`, `type -> subtype`
  - `structure_line`: `wharf_use -> structure_use`
  - `bridge_line`: `use_1 -> bridge_use`, `use_2 -> bridge_use2`
  - `water_point`: `temp -> temperature_indicator`
  - `landcover` / `landcover_line`: `track_use -> landcover_use`
  - `landuse` / `landuse_line`: `track_use -> landuse_use`, `track_type -> subtype`
  - `water`: `lake_use -> water_use`, `gazfeatid -> nzgb_feat_id`
  - `road_line`: `hway_num -> highway_number`, `num_lanes -> lane_count`, `lol_sufi -> rna_sufi`, `width -> width_indicator`

- ID normalization:
  - `UFID -> t50_fid` (integer)
  - `nz_topo50_map_sheet.t50id -> t50_fid` (integer)

### Current exclusions
- `contour` is skipped in this loader path (handled separately by contours/import_contours.py).

## 2) Post-load table/field management (postgis_manage_fields.py)

Execution order (`option="all"`):
1. `metadata`
2. `columns`
3. `name`
4. `null_updates`
5. `additions`
6. `road_lkp_updates`
7. `defaults`
8. `rename`
9. `carto_text_geom_update`
10. `recreate_table_srid`
11. `primary_key`
12. `process_carto_tables`

### Metadata step
- Adds metadata columns (default configuration):
  - `id uuid DEFAULT gen_random_uuid()`
  - `updated_at DATE DEFAULT CURRENT_DATE`
  - `created_at DATE DEFAULT CURRENT_DATE`

### Column consolidation and cleanup
- Moves values then drops old columns:
  - `structure_point`: `shaft_use -> structure_use`, `tank_type -> structure_type`, `materials -> material`
  - `structure_line`: `dam_status -> status`
  - `structure`: `species_cultivated -> species`, `reservoir_lid_type -> lid_type`, `tank_type -> structure_type`
  - `road_line`: `highway_numb -> highway_number`
  - `water`: `pond_use -> water_use`
- Drops `vegetation_point.name`.

### Value corrections
- `tunnel_line.tunnel_use2`: `'ivestock' -> 'livestock'`
- `tunnel_line.tunnel_use`: set `'vehicle'` where `tunnel_use2 = 'vehicle'`
- `tunnel_line.tunnel_use2`: set `'livestock'` where `tunnel_use2 = 'vehicle'`
- `trig_point.trig_type`: set to `'beaconed'`
- `road_line.way_count`: set `'one way'` where `way_count = '1'`
- `road_line.road_access`: set `'mp'` where `road_access = 'm'` (if column exists)
- `utility_line.support_type`: set `'pole'` where `type = 'telephone'`

### Null fill rules
- `runway.surface = 'grass'` where null
- `vegetation.species = 'coniferous'` where null and `type = 'exotic'`
- `railway_line.vehicle_type = 'train'` where null

### Added/derived fields
- `trig_point.code` from `trig_point.name`, then `trig_point.name` set to null
- `vegetation.subtype` from `vegetation.species`, then `vegetation.species` set to null
- `landcover.subtype`
- `road_line.hierarchy`, `road_line.width_indicator`, `road_line.name_id`
- `railway_line.route`, `railway_line.route2`, `railway_line.route3`
- `coastline.coastline_type`
- `water_line.hierarchy`, `water.hierarchy`

### Additional renames in this stage
- `contour.nat_form -> formation`
- `contour.designated -> designation`
- `landuse.track_type -> landuse_type`, then `landuse.landuse_type` populated from `visibility`, and `visibility` dropped
- `landuse_line.track_type -> landuse_type`
- `place_point.visibility -> place_type`
- `structure_line.materials -> material`
- `structure_line.mtlconveyd -> material_conveyed`
- `structure_point.store_item -> stored_item`
- `structure.store_item -> stored_item`

### Name field additions
- Adds `name` column when missing for:
  - `utility_point`
  - `utility_line`
  - `structure`
  - `ferry_crossing`

### Road lookup enrichment
- Updates `road_line.width_indicator` and `road_line.name_id` from `lookups.road_lkp` on `t50_fid`.

### Default values (DDL defaults)
- Sets defaults for selected fields including:
  - `runway.status = 'active'`, `runway.surface = 'sealed'`
  - type defaults on tables such as `airport`, `bridge_line`, `building`, `road_line`, `track_line`, `vegetation_point`, `trig_point`, `tunnel_line`, etc.

### Geometry handling and final CRS
- `nztopo50_map_sheet` geometry is snapped with `ST_SnapToGrid(geometry, 1.0)`.
- Most tables are re-created from EPSG:2193 to EPSG:4167 using `ST_Transform`.
- Excluded from SRID recreation: `collections`, `nztopo50_map_sheet`.

### Primary key and ESRI cleanup
- Drops `ESRI_OID` where present. Hang over from LAMPS export. May not be present in new download process.
- Primary key strategy (current default): UUID primary key on `id` using `gen_random_uuid()`.


## 3) Notes on legacy/disabled behaviour

- Island `location` generation (sea/inland) is not active in current code path.
- Contour loading is handled by the dedicated contours workflow, not by the main loader.


