# Load product data into topographic_product_data

General prerequisites information in README_PREREGQUISITES.md

*Pre-Step: Ensure schema has been created (manual step) in PostGIS database called carto*

Each of the product datasets has its own process to load...

CODE is in the **product** folder


# Process - Load Layers

**nz_topo50_carto_text** - added via import_carto_text.py script (SRID=NZTM2000 EPSG:2193)

The original file is exported from LAMPS as a shapefile.

**WARNING shp file fail to load some uft8 issue - convert to geojson first using UI (Pro/QGIS)**

source file becomes - "linz_carto_tex_FeaturesToJSO.geojson"

>Export to GEOJSON if needed - manual process


The import_carto_text script converts and field names and loads it into the carto schema.

Run: import_carto_text.py

*ADD QGIS Fields* - See Cartographic Text Post Processing and Re-Loading section below



**nz_topo50_map_sheet** - added via main topographic process and copied into carto schema (SRID=NZTM2000 EPSG:2193)

GRIDS

**nz_topo50_grid** - added via import_grids.py script (SRID=NZTM2000 EPSG:2193)

**nz_topo50_dms_grid** - added via import_grids.py script (SRID=WGS84 EPSG:4326)

# Create kart import script

Run the create_kartimport_file.py to create a .Bat file to run. Or just copy commands if you want to just do manually - only a few layers.

# Process - Kart Import and Push to repo

cd to work folder using command line that can run kart

kart init -b master topographic-product-data

## OR..

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


# Cartographic Text Post Processing and Re-Loading

Currently the initial cartographic text layer nz_topo_carto_text has an additional step applied.

Assumption is that the data is available and/or already loaded into repo.

This is expected to be a repeating process as the control speardsheet (rules) is updated to match the new fonts. More details can be found in the CARTO_TEXT_FIELDS_README.md file.

**Steps**

If loading directly into master the protection needs to be turned off

CLI - go to working folder - for example

> cd c:\data\topoedit

Clone last copy the the product repo and go into the folder. Note if starting from clean start point ie after running the import_carto_text step - Then the clone is not required.

> kart clone git@github.com:linz/topographic-product-data 

> cd topographic-product-data 

Step up python setting and spreadsheet

**Copy** the current GPKG - this will become the source.

Python file to check and run is *process_carto_text_newfields.py*

In the master GPKG - delete the nz_topo_carto_text layer

> kart data rm nz_topo_carto_text

Once the python file and supporting data is pointing at the correct files etc then... 

> Run the python file. A log is also created.

This will created an updated file in the master GPKG.

Check the log and output for example in QGIS - fields were updated.

Push the changes back to the master branch - typically this requires a force 

> kart push origin master --force

Once load it is worth cleaning up and re-cloning the branch to verify everything loaded ok.






kart add-dataset nz_topo50_carto_text -m "add carto text update"