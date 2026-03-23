# Load product data into topographic_product_data

General prerequisites information in README_PREREGQUISITES.md

*Pre-Step: Ensure schema has been created (manual step) in PostGIS database called carto*

Each of the product datasets has its own process to load...

CODE is in the **product** folder


# Process - Load Layers

**nz_topo50_carto_text** - added via import_carto_text.py script (SRID=NZTM2000 EPSG:2193)

The original file is exported from LAMPS as a shapefile.

The import_carto_text script converts and field names and loads it into the carto schema.

**WARNING** - loading from shp caused a failure so file was export to geojson.
shp file fail to load some uft8 issue - convert to geojson first using UI (Pro/QGIS)

>Export to GEOJSON if needed - manual process

Run: import_carto_text.py

*ADD QGIS Fields*

March 2026 - pre-processing code for this layer is under development. The process will add fields need for QGIS mapping and update values based in carto_text based on lookups that are currently being created. Process to be defined....


**nz_topo50_map_sheet** - added via main topographic process and copied into carto schema (SRID=NZTM2000 EPSG:2193)

GRIDS

**nz_topo50_grid** - added via import_grids.py script (SRID=NZTM2000 EPSG:2193)

**nz_topo50_dms_grid** - added via import_grids.py script (SRID=WGS84 EPSG:4326)

# Create kart import script

Run the create_kartimport_file.py to create a .Bat file to run. Or just copy commands if you want to just do manually - only a few layers.

# Process - Kart Import and Push to repo

cd to work folder using command line that can run kart

kart init -b master topographic-product-data

OR..

if work flow is going to load to branch and then PR into master create a branch (reflect release version)

>Make sure it is a new branch

kart init -b release64 topographic-data

THEN...

cd topographic-product-data

kart remote add origin git@github.com:linz/topographic-product-data

CHECK the remote is correct

kart remote -v

Note: Current the data can be loaded in one go. Memory/Timeout ok. If more add at some point may need to follow topo-data approach and do import/push in bat file.

Run BAT import or run commands manually - just does the import steps

kart push origin master [--force] --force use if going to master and need to clear repo. Skip if going to branch.


