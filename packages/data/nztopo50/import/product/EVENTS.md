# Product Layer Events

This file summarizes what happens to each layer across the Python scripts in the Product.

## Workflow 

**import_map_sheet.py** does not only load map sheet data. It also enriches and restructures `carto.nztopo50_carto_text` using map-sheet derived lookups.
**process_carto_text_newfields.py** is a separate product-output styling pass over `nztopo50_carto_text` in GeoPackage form.

The **import_map_sheet** process requires the topographic layers to have been loaded as it uses trig and geographic_names. Also the cartographic_text.

**Load order**

*nztopo50_carto_text* - loads data

*import_map_sheet* - loads and updates new fields with relates IDs and then dropsome fields. Complex processing.

*process_carto_text_newfields* - can run anythime after main data loaded. Add new fields based on rules set. Original info provides via excel and reformatted into CSV file in source control.

*nztopo50_grid and nztopo50_dms_grid* - loads data


## nztopo50_carto_text

### Source and load
- Script: `import_carto_text.py`
- Reads `linz_carto_text` from a GeoPackage (or Shapefile/GeoJSON depending on source mode).
- Renames truncated source fields to full names:
  - `t_c_s_d` -> `text_char_spacing_distance`
  - `text_colou` -> `text_colour`
  - `text_heigh` -> `text_height`
  - `text_orien` -> `text_orientation`
  - `text_place` -> `text_placement`
  - `txt_size_t` -> `text_size_type`
  - `txt_s_l` -> `text_stretch_length`
  - `text_strin` -> `text_string`
  - `t_w_s_d` -> `text_word_spacing_distance`
- Drops `FID` when present.
- Enforces field types:
  - strings: `full_text`, `text_font`, `text_string`
  - ints: `text_bend`, `text_colour`, `text_placement`, `text_size_type`
  - floats: spacing/stretch fields
  - rounds `text_height` to 4 dp and `text_orientation` to 2 dp
- Adds UUID `id` per feature.
- Writes to PostGIS table `carto.nztopo50_carto_text` with `if_exists='replace'`.
- Adds primary key on `id`.

### Load and Enrichment from map sheet matching (also impacts carto_text)


## lookups.example_point_ids (lookup table)

- Script: created as part of `import_map_sheet.py` process
- Recreated each run (`DROP TABLE IF EXISTS` then `CREATE TABLE AS`).
- Combines trig point and geographic name example IDs into one table with geometry transformed to EPSG:2193.
- Used to map `carto.nztopo50_carto_text.full_text` to `example_point_id` (first by unique name, then by nearest geometry).

## map sheet and carto text processing

- Script: `import_map_sheet.py`
- Reads `linz_map_sheet` from source package.
- Renames fields (for example `ex_name` -> `example_name`, `ex_class` -> `example_class`).
- Drops `FID` when present.
- Casts key fields to stable types.
- Adds UUID `id` and sets `type = 'nztopo50_map_sheet'`.
- Derives origin coordinates from geometry:
  - `x_origin` = min x of geometry bounds (rounded)
  - `y_origin` = max y of geometry bounds (rounded)
- Derives publication metadata from `edition`:
  - `published_version` from `Edition <n>`
  - `published_at` from `Published <year>` as `<year>-01-01`
  - `updated_at` initialized from `published_at`
- Writes to PostGIS as `carto.nztopo50_map_sheet` with `if_exists='replace'` and typed columns.
- Adds primary key on `id`.
- Deletes records with `sheet_code LIKE 'Topo%'`.
- Populates `example_point_id` by joining to release schema data:
  - `release66.trig_point` where `example_class = 'trig_pnt'` and code matches name
  - `release66.geographic_name` where `example_class = 'geographic_name'` and name matches
- Applies data fixes on unmatched names/codes:
  - `Mt ...` -> `Mount ...`
  - code remaps (`A0TR` -> `A0U2`, `AP8Y` -> `A4UX`)
  - selected macron name corrections
- Re-runs example-point matching after fixes.
- Optionally drops temporary columns (`example_name`, `example_class`, `edition`, `revised`) when `final_drop_fields=True`.

- Adds `example_point_id UUID` to `carto.nztopo50_carto_text` (if missing).
- Updates `example_point_id` in two passes:
  - name-based update for unambiguous matches using `lookups.example_point_ids`
  - geometry-based update for ambiguous names using nearest point within 200m (`ST_DWithin` + `ST_Distance`)
- Reorders table columns so `example_point_id` is second after `id` by rebuilding table.
- Re-adds primary key on `id` after rebuild.

### Cartographic field augmentation for product output
- Script: `process_carto_text_newfields.py`
- Reads layer from product GeoPackage, normalizes geometry type by converting `LineString` to `MultiLineString` where needed.
- Adds new fields with defaults and constraints:
  - `font`, `style`, `colour`, `size`, `placement`, `offset`, `textanchor`, `labelanchor`, `charplace`, `chardistance`
- Builds matching logic from mapping spreadsheet tabs (`Full layers`, `New values`, `Font mapping`) via processed CSVs.
- Parses SYMBOL expressions (for example font/colour/height/placement conditions), queries matching features, groups by `text_bend`, then assigns new output styling fields.
- Where no style mapping is found, sets new fields to empty/zero defaults.
- Exports updated `nztopo50_carto_text` back to output GeoPackage.

## nztopo50_grid

- Script: `import_grids.py`
- Reads `grid` layer from `grid.gpkg`.
- Adds UUID `id` per feature.
- Writes to PostGIS as `carto.nztopo50_grid` with `if_exists='replace'`.
- Sets `id` default to `gen_random_uuid()`.

## nztopo50_dms_grid

- Script: `import_grids.py`
- Reads `dms_grid_3` layer from `dms_grid_3.gpkg`.
- Adds UUID `id` per feature.
- Writes to PostGIS as `carto.nztopo50_dms_grid` with `if_exists='replace'`.
- Sets `id` default to `gen_random_uuid()`.





