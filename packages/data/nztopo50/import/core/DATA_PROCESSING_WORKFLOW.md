# Data Processing Workflow

Last checked: 2026-07-14

This document is a refresh of the current executable workflow using:
- core/load_shp_to_themes.py
- core/postgis_manage_fields.py

The flow below is event-based and numbered so each change event can be traced back to code.

## A. Loader Event Sequence (load_shp_to_themes.py)

Current run configuration in `run()`:
- source_mode = gpkg_files
- target = postgis
- schema_name = output value (typically release schema)

### Event 01: Build layer mapping
1. Read layers_info.csv.
2. Register both shp_name and kart_layer_name as valid keys.
3. Store mapping payload: object_name, theme, type, layer_name, dataset, kart_layer_name.

### Event 02: Discover source files
1. List files from local object store path.
2. Filter to .gpkg files for current run mode.

### Event 03: Group files by logical output layer
1. Read source metadata with pyogrio.read_info.
2. Resolve layer_name from mapping.
3. Group source field arrays by layer_name.

### Event 04: Compute harmonized field set per layer
1. Union all field sets for each layer.
2. Save this union as the common schema for that layer.

### Event 05: Read and normalize per source file
1. Load feature data with geopandas.read_file.
2. Resolve output layer from mapping.
3. Skip contour in this loader path (contours are managed by contours/import_contours.py).
4. Add any missing common fields as NULL.
5. Add feature_type from mapping type.
6. Reproject to EPSG:2193.
7. Reorder columns so geometry is last.

### Event 06: Apply rename and value normalization rules
1. Run reset_column_names(layer_name) for layer-specific and generic rename rules.
2. Normalize identifiers:
- UFID -> t50_fid (int)
- nz_topo50_map_sheet.t50id -> t50_fid (int)
3. Convert feature_type -> type after layer-specific remaps are applied.

### Event 07: Layer-specific mapping highlights
1. tunnel_line: use1 -> tunnel_use, use2 -> tunnel_use2, type -> subtype.
2. structure: type -> subtype.
3. structure_point: use -> structure_use, type -> subtype.
4. bridge_line: use_1 -> use1, use_2 -> use2.
5. road_line: hway_num -> highway_number, num_lanes -> lane_count (int), lol_sufi -> rna_sufi (int), width -> width_indicator.
6. water: lake_use -> subtype, gazfeatid -> feat_id.
7. runway: runway_use -> subtype.
8. track_line: track_use -> subtype.

### Event 08: Generic short-field expansion highlights
1. compositn -> composition
2. descriptn -> description
3. info_disp -> info_display
4. veh_type -> vehicle_type
5. restrictns -> restrictions
6. orientatn -> orientation
7. constr_typ -> construction_type
8. support_typ/support_ty -> support_type
9. bldg_use/pipe_use/rway_use/embkmt_use -> subtype
10. grp_ascii/grp_macron/grp_name -> group_ascii/group_macron/group_name
11. substance -> substance_extracted

### Event 09: Persist layer to PostGIS
1. Log row count per layer.
2. Append into schema.table via GeoDataFrame.to_postgis.
3. Continue through all discovered files.

## B. Post-load Change Events (postgis_manage_fields.py)

When option=all, workflow executes 15 ordered change events:

### Event 10: metadata
1. Add metadata fields across schema tables (if missing):
- id uuid default gen_random_uuid()
- updated_at date default current_date
- created_at date default current_date
- metadata varchar(1000) default ''

### Event 11: columns
1. Consolidate values from source columns into base columns, then drop source columns.
2. Includes structure_point, structure_line, structure, road_line, water, bridge_line mappings.
3. Drop vegetation_point.name.
4. Apply direct fixes:
- tunnel_line: ivestock -> livestock fix chain
- trig_point.trig_type -> beaconed
- road_line.way_count: 1 -> one way
- road_line.road_access: m -> mp (if present)
- utility_line.support_type -> pole where type=telephone

### Event 12: name
1. Add name column when missing for:
- utility_point
- utility_line
- structure
- ferry_line

### Event 13: null_updates
1. Fill defined nulls:
- runway.surface = grass
- vegetation.species = coniferous where type=exotic
- railway_line.vehicle_type = train

### Event 14: additions
1. Add and derive trig_point.code from name, then clear trig_point.name.
2. Add vegetation.subtype from species, then clear vegetation.species.
3. Add landcover.subtype.
4. Add landcover_point.subtype from display code mapping, then drop display.
5. Add road_line.hierarchy and width_indicator.
6. Add water_line.hierarchy and water.hierarchy.
7. Rename bridge_line.use2 -> subtype.
8. Rename contour.nat_form -> formation.
9. Rename contour.designated -> designation.
10. Rename landuse.track_type -> landuse_type, populate from visibility, drop visibility.
11. Rename landuse_line.track_type -> landuse_type.
12. Rename landuse_point.visibility -> subtype.

### Event 15: road_lkp_updates
1. Update road_line.width_indicator from lookups.road_lkp by t50_fid.

### Event 16: defaults
1. Set DDL defaults for selected fields.
2. Examples:
- runway.status default active
- runway.surface default sealed
- type defaults for airport, bridge_line, building, railway_line, road_line, track_line, vegetation_point, trig_point, tunnel_line

### Event 17: rename
1. structure_line.materials -> material
2. structure_line.mtlconveyd -> material_conveyed
3. structure_point.store_item -> stored_item
4. structure.store_item -> stored_item
5. water.temperature -> temperature_indicator

### Event 18: structure_updates
1. Re-map structure subtype/type logic for bivouac, lighthouse/beacon, shaft/windmill, gate, tank, wreck, cableway cases.
2. Fix typo subtype watre -> water.
3. Normalize water subtype hydro-electric -> hydro_electric.
4. Drop transitional columns used for remapping.

### Event 19: use_to_subtype_updates
1. water.subtype <- water_use where water_use=hydro-electric.
2. Add ferry_line.subtype and set vehicle.
3. Promote landuse_use into type for landuse/landuse_line.
4. Normalize landuse type values: horse_track, vehicle_track, cycle_track, dog_track.
5. Normalize landuse subtype historic -> old.
6. Drop landuse_use columns from landuse and landuse_line.
7. Populate metadata JSON for water (NZGB lineage) and road_line (AIMS lineage).
8. Drop water.feat_id and road_line.rna_sufi after metadata capture.

### Event 20: update_spaces_with_underscores
1. Replace spaces with underscores for configured table/column pairs in TABLE_UNDERSCORE_COLUMNS.

### Event 21: carto_text_geom_update
1. Snap nztopo50_map_sheet.geometry to grid using ST_SnapToGrid(geometry, 1.0).

### Event 22: recreate_table_srid
1. Recreate most schema tables from SRID 2193 to 4167 via ST_Transform.
2. Skip collections and nztopo50_map_sheet.
3. Rebuild geometry and selected attribute indexes.
4. Re-apply metadata defaults with alter mode.

### Event 23: primary_key
1. Drop ESRI_OID where present.
2. Rebuild primary key on id.
3. Default mode is UUID primary key with gen_random_uuid().

### Event 24: process_carto_tables
1. Currently no-op in standard release runs (tables_to_copy is empty).

## C. Change Event Count Summary

1. Loader events: 9
2. Post-load events: 15
3. Total numbered events: 24

## D. Operational Notes

1. Contour loading is intentionally excluded from load_shp_to_themes.py and handled in contours/import_contours.py.
2. Event 24 is retained in sequence for completeness, but does not move data unless tables_to_copy is populated.


