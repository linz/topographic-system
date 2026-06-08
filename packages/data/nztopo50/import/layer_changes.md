# Layer changes notes

## Scope
- Source files reviewed:
  - core/load_shp_to_themes.py
  - core/postgis_manage_fields.py
- Focus layers: road_line 

## load_shp_to_themes.py

### road_line specific changes
- In reset_column_names, when layer_name is road_line:
  - Rename hway_num to highway_number.
  - Rename num_lanes to lane_count, fill nulls with 0, cast to int.
  - Rename lol_sufi to rna_sufi, fill nulls with 0, cast to int.
  - Drop RW_lane_c when present.
  - Rename width to width_indicator.
  - Drop RW_surface when present.

### generic layer processing that can affect road_line 
- During process_and_save_layers:
  - Missing common columns are added as null before save.
  - feature_type is set from layers_info mapping.
  - Geometry is reprojected to EPSG:2193.
  - Columns are reduced/reordered to common columns + feature_type + geometry.
  - Data is written to PostGIS table named by layer_name in the selected schema.


## postgis_manage_fields.py

### workflow order (relevant to road tables)
- The run method executes steps in this order:
  - metadata
  - columns
  - name
  - null_updates
  - additions
  - road_lkp_updates
  - defaults
  - rename
  - carto_text_geom_update
  - recreate_table_srid
  - primary_key
  - process_carto_tables

### road_line specific changes
- columns step:
  - set_base_column_and_drop_column on road_line: highway_number <- highway_numb, then drop highway_numb.
  - Update way_count values: where way_count = '1', set to 'one way'.
  - If road_access column exists: where road_access = 'm', set to 'mp'.
- additions step:
  - Add hierarchy VARCHAR(50).
  - Add width_indicator VARCHAR(5).
  - Add name_id BIGINT.
  - Add hierarchy VARCHAR(25) again later in the same step.
- road_lkp_updates step:
  - Update road_line from lookups.road_lkp by t50_fid:
    - width_indicator = l.width_indicator
    - name_id = l.name_id
- defaults step:
  - Set road_line.feature_type default to 'road'.

### generic workflow changes that can affect road_line
- metadata step:
  - add_metadata_columns runs across tables in the schema.
- null_updates step:
  - populate_defined_null_values runs across tables in the schema.
- recreate_table_srid step:
  - Recreates each table in schema from SRID 2193 to 4167, rebuilds geometry type and indexes.



