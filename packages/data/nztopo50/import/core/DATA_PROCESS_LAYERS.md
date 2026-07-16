# Data Process Layers

Last checked: 2026-07-15

This document maps processing by layer using:
- core/load_shp_to_themes.py
- core/postgis_manage_fields.py

It is intended as a layer-by-layer companion to DATA_PROCESSING_WORKFLOW.md.

## Global Processing (All Layers Unless Noted)

### 1. Loader stage (load_shp_to_themes.py)
1. Source files are discovered in gpkg mode by default.
2. Files are mapped to logical layer_name via layers_info.csv (shp_name and kart_layer_name keys).
3. A union of all source fields is computed per logical layer; missing fields are added as NULL.
4. feature_type and theme are added from feature_type and theme mapping.
5. Data is reprojected to EPSG:2193.
6. Column order is normalized (geometry last).
7. Common short-name fixes are applied when present:
   - compositn -> composition
   - descriptn -> description
   - info_disp -> info_display
   - veh_type -> vehicle_type
   - restrictns -> restrictions
   - orientatn -> orientation
   - constr_typ -> construction_type
   - support_typ/support_ty -> support_type
   - bldg_use/pipe_use/rway_use/embkmt_use -> subtype
   - grp_ascii/grp_macron/grp_name -> group_ascii/group_macron/group_name
   - substance -> substance_extracted
8. UFID -> t50_fid (integer) when present.
9. feature_type -> type (after any layer-specific type/subtype remaps).
10. Data is appended to schema.table in PostGIS.

### 2. Post-load stage (postgis_manage_fields.py, option=all)
1. Metadata columns are added/normalized across schema tables: id, updated_at, created_at, metadata.
2. Generic value cleanup steps run in fixed order:
   - columns
   - name
   - null_updates
   - additions
   - road_lkp_updates
   - defaults
   - rename
   - structure_updates
   - use_to_subtype_updates
   - update_spaces_with_underscores
   - carto_text_geom_update
   - recreate_table_srid
   - primary_key
   - process_carto_tables
3. Most tables are transformed from SRID 2193 to 4167.
4. Primary key is set to id (UUID by default).

## Layer-by-Layer Processing

For each layer:
- Loader: layer-specific behavior in load_shp_to_themes.py.
- Post-load: layer-specific behavior in postgis_manage_fields.py.
- If no explicit layer-specific rule exists, the layer receives Global Processing only.

### airport
- Loader:
  - Global Processing only.
- Post-load:
  - defaults: type default set to 'airport'.
  - update_spaces_with_underscores applies to type.

### bridge_line
- Loader:
  - use_1 -> use1
  - use_2 -> use2
- Post-load:
  - columns: type <- use1, then use1 dropped.
  - additions: use2 renamed to subtype.
  - defaults: type default set to 'bridge'.
  - update_spaces_with_underscores applies to type, use1, use2, construction_type, status.

### building
- Loader:
  - bldg_use -> subtype (generic short-field rule)
- Post-load:
  - defaults: type default set to 'building'.
  - update_spaces_with_underscores applies to type, subtype, status.

### building_point
- Loader:
  - bldg_use -> subtype (generic short-field rule)
- Post-load:
  - defaults: type default set to 'building'.
  - update_spaces_with_underscores applies to type, subtype, status.

### coastline
- Loader:
  - Global Processing only.
- Post-load:
  - update_spaces_with_underscores applies to type.

### contour
- Loader:
  - Skipped intentionally in this loader path.
  - Contours are expected from contours/import_contours.py.
- Post-load:
  - additions: nat_form -> formation, designated -> designation.
  - update_spaces_with_underscores applies to type, definition, designation, formation.

### descriptive_text
- Loader:
  - info_disp -> info_display (generic short-field rule)
- Post-load:
  - defaults: type default set to 'descriptive_text'.
  - update_spaces_with_underscores applies to type.

### fence_line
- Loader:
  - Global Processing only.
- Post-load:
  - update_spaces_with_underscores applies to type.

### ferry_line
- Loader:
  - Global Processing only.
- Post-load:
  - name: ensures name column exists.
  - use_to_subtype_updates: adds subtype and sets subtype='vehicle'.
  - defaults: type default set to 'ferry_crossing'.
  - update_spaces_with_underscores applies to type.

### geographic_name
- Loader:
  - Global Processing only.
- Post-load:
  - update_spaces_with_underscores applies to type.

### island
- Loader:
  - Global Processing only.
- Post-load:
  - update_spaces_with_underscores applies to type.

### landcover
- Loader:
  - track_use -> landcover_use
- Post-load:
  - additions: ensures subtype column exists.
  - update_spaces_with_underscores applies to type, subtype.

### landcover_line
- Loader:
  - track_use -> landcover_use
- Post-load:
  - update_spaces_with_underscores applies to type.

### landcover_point
- Loader:
  - Global Processing only.
- Post-load:
  - additions: adds subtype; derives subtype from display code mapping; drops display.
  - update_spaces_with_underscores applies to type, subtype.

### landuse
- Loader:
  - track_use -> landuse_use
  - track_type -> subtype
- Post-load:
  - additions: track_type -> landuse_type; landuse_type <- visibility; drops visibility.
  - use_to_subtype_updates:
    - type <- landuse_use where present.
    - subtype 'historic' -> 'old'.
    - drops landuse_use.
  - update_spaces_with_underscores applies to type, subtype, status, substance_extracted.

### landuse_line
- Loader:
  - track_use -> landuse_use
  - track_type -> subtype
- Post-load:
  - additions: track_type -> landuse_type.
  - use_to_subtype_updates:
    - type <- landuse_use where present.
    - type normalization: horse -> horse_track, vehicle -> vehicle_track, cycle -> cycle_track, dog -> dog_track.
    - drops landuse_use.
  - update_spaces_with_underscores applies to type, subtype.

### landuse_point
- Loader:
  - Global Processing only.
- Post-load:
  - additions: visibility renamed to subtype.
  - update_spaces_with_underscores applies to type, status, subtype, substance_extracted.

### marine
- Loader:
  - compositn -> composition (generic short-field rule)
- Post-load:
  - update_spaces_with_underscores applies to type, composition.

### marine_point
- Loader:
  - compositn -> composition (generic short-field rule)
- Post-load:
  - Global Processing only.

### nztopo50_map_sheet
- Loader:
  - t50id -> t50_fid (integer)
  - ex_class -> example_class
  - ex_name -> example_name
- Post-load:
  - carto_text_geom_update: geometry snapped to 1m grid.
  - excluded from recreate_table_srid step.

### place_point
- Loader:
  - compositn -> composition (generic short-field rule)
- Post-load:
  - update_spaces_with_underscores applies to type, composition.

### railway_line
- Loader:
  - rway_use -> subtype (generic short-field rule)
  - veh_type -> vehicle_type (generic short-field rule)
- Post-load:
  - null_updates: vehicle_type set to 'train' where NULL.
  - defaults: type default set to 'railway'.
  - update_spaces_with_underscores applies to type, subtype, track_type, vehicle_type, status.

### railway_point
- Loader:
  - Global Processing only.
- Post-load:
  - defaults: type default set to 'station'.
  - update_spaces_with_underscores applies to type.

### relief
- Loader:
  - Global Processing only.
- Post-load:
  - update_spaces_with_underscores applies to type.

### relief_line
- Loader:
  - Global Processing only.
- Post-load:
  - update_spaces_with_underscores applies to type, subtype.

### relief_point
- Loader:
  - Global Processing only.
- Post-load:
  - update_spaces_with_underscores applies to type.

### residential_area
- Loader:
  - Global Processing only.
- Post-load:
  - defaults: type default set to 'residential_area'.
  - update_spaces_with_underscores applies to type.

### road_line
- Loader:
  - hway_num -> highway_number
  - num_lanes -> lane_count (NULL -> 0, cast integer)
  - lol_sufi -> rna_sufi (NULL -> 0, cast integer)
  - width -> width_indicator
  - drops RW_lane_c and RW_surface if present
- Post-load:
  - columns: highway_number <- highway_numb, then highway_numb dropped.
  - columns: way_count '1' -> 'one way'.
  - columns: road_access 'm' -> 'mp'.
  - additions: ensures hierarchy, width_indicator, road_access columns exist.
  - road_lkp_updates: width_indicator and road_access updated from lookups.road_lkp by t50_fid.
  - defaults: type default set to 'road'.
  - use_to_subtype_updates: metadata JSON populated from LINZ AIMS lineage; drops rna_sufi after metadata write.
  - update_spaces_with_underscores applies to type, hierarchy, status, surface, width_indicator.

### runway
- Loader:
  - runway_use -> subtype
- Post-load:
  - null_updates: surface='grass' where NULL.
  - defaults: status default='active', surface default='sealed'.
  - update_spaces_with_underscores applies to type, subtype, status, surface.

### structure
- Loader:
  - type -> subtype
- Post-load:
  - columns: species <- species_cultivated; lid_type <- reservoir_lid_type; structure_type <- tank_type (source columns dropped).
  - name: ensures name column exists.
  - rename: store_item -> stored_item.
  - structure_updates: subtype <- stored_item where type='tank'; then stored_item dropped.
  - update_spaces_with_underscores applies to lid_type, subtype, species, status.

### structure_line
- Loader:
  - wharf_use -> subtype
- Post-load:
  - columns: status <- dam_status (dam_status dropped).
  - rename: materials -> material; mtlconveyd -> material_conveyed.
  - structure_updates:
    - subtype <- material_conveyed where type='cableway_industrial'.
    - subtype <- restrictions where type='cableway_people'.
    - drops material, material_conveyed, restrictions after remap.
  - update_spaces_with_underscores applies to type, subtype, species, status.

### structure_point
- Loader:
  - use -> structure_use
  - type -> subtype
- Post-load:
  - columns: structure_use <- shaft_use; structure_type <- tank_type; material <- materials (source columns dropped).
  - rename: store_item -> stored_item.
  - structure_updates:
    - subtype <- material for type in (bivouac, tower).
    - subtype='building' for bivouac where subtype is null.
    - type <- subtype where subtype='lighthouse'.
    - subtype <- location for type in (lighthouse, beacon).
    - subtype <- structure_use for type in (shaft, windmill).
    - subtype <- restrictions for type='gate'.
    - subtype <- stored_item for type='tank'.
    - subtype <- wreck_of for type='wreck'.
    - typo fix: subtype 'watre' -> 'water'.
    - drops transitional fields material, location, structure_use, restrictions, stored_item, wreck_of.
  - update_spaces_with_underscores applies to type, subtype, status.

### track_line
- Loader:
  - track_use -> subtype
- Post-load:
  - defaults: type default set to 'track'.
  - update_spaces_with_underscores applies to type, subtype, track_type, status.

### transport_point
- Loader:
  - Global Processing only.
- Post-load:
  - update_spaces_with_underscores applies to type.

### trig_point
- Loader:
  - Global Processing only.
- Post-load:
  - columns: trig_type set to 'beaconed'.
  - additions: add code, copy from name where code is null, then clear name.
  - defaults: type default set to 'trig'.
  - update_spaces_with_underscores applies to type, trig_type.

### tunnel_line
- Loader:
  - use1 -> tunnel_use
  - use2 -> tunnel_use2
  - type -> subtype
- Post-load:
  - columns typo/normalization:
    - tunnel_use2 'ivestock' -> 'livestock'
    - if tunnel_use2='vehicle', then tunnel_use='vehicle' and tunnel_use2='livestock'
  - defaults: type default set to 'tunnel'.
  - update_spaces_with_underscores applies to type, tunnel_use, tunnel_use2, subtype.

### utility_line
- Loader:
  - support_typ/support_ty -> support_type (generic short-field rule)
  - pipe_use -> subtype (generic short-field rule)
- Post-load:
  - columns: support_type='pole' where type='telephone'.
  - name: ensures name column exists.
  - update_spaces_with_underscores applies to type, subtype, support_type, status, visibility.

### utility_point
- Loader:
  - Global Processing only.
- Post-load:
  - name: ensures name column exists.
  - update_spaces_with_underscores applies to type.

### vegetation
- Loader:
  - Global Processing only.
- Post-load:
  - null_updates: species='coniferous' where species is null and type='exotic'.
  - additions: add subtype <- species, then clear species.

### vegetation_line
- Loader:
  - Global Processing only.
- Post-load:
  - Global Processing only.

### vegetation_point
- Loader:
  - Global Processing only.
- Post-load:
  - columns: drops name.
  - defaults: type default set to 'tree'.
  - update_spaces_with_underscores applies to type.

### water
- Loader:
  - lake_use -> subtype
  - gazfeatid -> feat_id
  - temperature -> temperature_indicator
- Post-load:
  - columns: subtype <- pond_use (pond_use dropped).
  - rename: temperature -> temperature_indicator (defensive rename step).
  - additions: ensures hierarchy column exists.
  - structure_updates: subtype 'hydro-electric' -> 'hydro_electric'.
  - use_to_subtype_updates:
    - subtype <- water_use where water_use='hydro-electric'.
    - metadata JSON populated from NZGB Gazetteer lineage.
    - drops feat_id after metadata write.
  - update_spaces_with_underscores applies to type, subtype, hierarchy, perennial, temperature_indicator.

### water_line
- Loader:
  - Global Processing only.
- Post-load:
  - additions: ensures hierarchy column exists.
  - update_spaces_with_underscores applies to type.

### water_point
- Loader:
  - temp -> temperature_indicator
  - generic temp -> temperature rule may also apply when temp exists.
- Post-load:
  - update_spaces_with_underscores applies to type, temperature_indicator.

## Quick Run Order

1. Run load_shp_to_themes.py to load and normalize source layers into release schema.
2. Run postgis_manage_fields.py with option=all on the same schema.
3. Validate row counts and key post-load outputs (metadata, PK, SRID, renamed/dropped columns).
