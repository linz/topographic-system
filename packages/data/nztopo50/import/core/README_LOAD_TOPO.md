# Create Model

The topo50 model can be created dynamically using these scripts and control spreadsheets

These scripts use the postgis database version although the load script is currently generic.

The layer info spreadheet **model\layer_info.xlsx** defines the LAMPS objects into themes and datasets and new layer names. A CSV version is in the core folder.

The field mapping spreadsheet **model\dataset_fields.xlsx** lists all the fields and mappings by layer.
Setting up data model and loading data into Postgres. A CSV version is in the core folder.

**This process is currently manually run.
The default location for the configuration files is c:\data\model**

To help identify change the 2 configuration files - dataset_fields.xlsx and layers_info.xlsx are copied into this folder. They have been saved as CSV files in the repo.

The code has a default database "topo" and password. If different ones used these will need to be changed.

**re: products**

See products/README_LOAD_PRODUCT.md for instructions on loading and processing product datasets.

## Process

## The following PRE-PROCESS STEPS are required

**POSTGRES/POSTGIS** - check load schema has been manually created (named after release) for example release65

**POSTGRES/POSTGIS** - check carto schema has been manually created called carto - some core data is copied into carto (currently map_sheets)

**See: processing_steps.py - for code and manual SQL to use for roads with 0 as t50_fid.**

Replace lds export with LAMPS direct export

**road_cl.shp** - this is a direct export from LAMPS as it contains additional fields

If road_cl.cpg exists - delete or rename - read has issue if it exists.

Also **CHECK** if it has to50_fid still set to 0. The pre-processing.py script has a SQL command to update the IDS once in Postgres.

If road_line - t50_fid in database has 0 values - run SQL to assign a value. Mainly used when aligning topo_id between releases.

**islands_poly.shp** - this needs the additional field (location) and calculation of offshore (1) or inland island (0) - added using the pre_processing_steps.py script. The sea_coastline poly shapefile create from coastline and outer box

## MAIN Automated Steps

_Pre-Step: Ensure schema has been created (manual step) in PostGIS database after version target for example release64_

_Step 1: Create schemas in PostGIS_

    Note: Typically done manually. Example code: postgis_create_schemas.py

_Step 2: Create Model in PostGIS_ - Note: option to DROP tables available. command_option = 'drop_tables'. Check database after to ensure any changes didn't delete a table. Delete manually if that is the case.

    Run script manually - postgis_create_model.py
    set the command_option = 'create_model'
    check model_fields_file is pointing to the dataset_fields.xlsx file
    check the data load schema name - schema_name = "release62"
    by default - primary_key_type = 'uuid'

_Step 3: Load Shapefiles into PostGIS_

    Run script manually - load_shp_to_themes.py
    pyproj (project database) - had issue locating this after qgis installed. This can be commented out if not an issue.
    check layer_info_file is pointing to the layers_info.xlsx file
    set the release value - release=release62 - This should be the same as the schema in create model step and folder name containing data to load or hard code
    set data_folder location - Expects LDS source shapefiles exported from LAMPS. Needs to confirm/adapt if other approach used.

_Step 3a: Load contours into PostGIS_

    **Note:** If using the LDS contours they have Z values in the geometry. Currently we are dropping this.

    **If new dataset - run the drop_z_from_source.py script.**

    Run script under contours - import contours.py - this uses the LDS version of contours which covers all of NZ.

_Step 4: Create Indexes in PostGIS (optional)_
Run script manually - postgis_create_model.py
set the command_option = 'create_indexes'

_Step 5: Add additional feature metadata fields and Reproject to EPSG:4167 and restructre field order, update nulls etc in PostGIS_

    Run script manually - postgis_manage_fields.py
    option should be set to 'all'
    set correct schema_name - "release62"
    set a date for the release - release_date = "2025-02-05"

    defaults
    add_full_metadata_fields = True
    primary_key_type = 'uuid'

    If you are loading mulitple generations of data we need to reset GUID - process the prvious release information (data should be in same database - previous release schema)
    update_topoid_from_previous_release = False / True
    previous_schema = "release61"

    This process will automatically generate change datasets. Can use a hive version.
    use_hive_partitioning = True
    change_logs_path = "c:\data\topo-data-vector\changelogs"

_Note: Apply Constraints_ -while this is code in database_rules - the approach will be to apply this via QGIS

## Sync TOPO_ID - Optional BUT useful if running the changelog process or uploading a new version of data to repo rather than full replacement

The t50_fid processed for LDS datasets is a life value (same feature same id). The topo_id is newly created on load.

To sync the TOPO_ID (as the new primary key) between 2 releases use the **sync_topo_ids.py** script. This does a join based on the t50_fid. If there are any 0's in the t50_fid this will throw an error (see roads pre-processing). The script can safely be re-run.

## Loaded Data Checker 

The script loaded_data_checcker.py can be run to check the data meets the change expectations.

# Kart Import and Push

More information - https://toitutewhenua.atlassian.net/wiki/spaces/LI/pages/1171718236/Kart+Data+Import+and+Environment

Repeat as needed for:

> topographic-data

> topographic-contour-data

## Clean existing repo

If data is in an existing repo and this is a clean rebuild follow the following steps:

_In GitHub_

Remove any branches

Delete the Branch protection rules on the master branch - Settings / Branch - NOTE: you need correct permissions

We want to remove the old data this is down by using force - this disconnects the data and backend clean up will delete it. The delete can be forced.

Open your CLI (supporting Kart) and go to your work area - for example c:\data\toposource

Create an empty kart repo and run a push

> kart init -b master topographic-data

> cd topographic-data

> kart remote add origin git@github.com:linz/topographic-data [or target repo for other data]

> kart remote -v

This will be an empty repo.

Import a small datasets - this will then be deleted.

> kart import postgresql://postgres:landinformation@localhost/topo/release64 --primary-key topo_id airport

> kart push origin master --force

> kart data rm airport

> kart push origin master

**CHECK the import BAT file has the correct settings**

1. tree_locations should be commented out (default) or removed if loading via manual method

2. --force option - this clear the repo of all data and code. If loading into a branch make sure the --force is not set (default)

3. Confirm you are point to the correct schema - e.g /release64

4. Confirm you are pushing to the correct branch e.g kart push origin master **versus** kart push origin release64

## Main Process - Approach 1

Approach 1 - In the clean repo - Add any requirments before load.

Typically this is the ACTIONS code.

Could include a README.md file to document current load (not these files are not visible via Kart Download)

In this approach we clone the repo

> Using CLI (Kart enabled) - cd to working folder. For example c:\data\toposource

> kart clone git@github.com:linz/topographic-data [or target repo for other data]

Check all ok

> kart remote -v

> copy the kart_import_topodata.bat into the topographic-data folder and check settings correct - SEE: **CHECK the import BAT file has the correct settings**

DO first: Run the tree_locations manually first sometimes it works. If not the the README_TREE_LOCATIONS.md instructions. Best to do this work-around first. If it fails you may need to delete the topographic-data folder and contents and redo steps from the Kart Import instructions (this section)

Run: bat file (windows). This does a push after each load so don't hit timeout issues.

When ready - run...

@kart_import_topodata.bat

## Main Process - Approach 2

Approach 2 - We do a force which removes all content of the repo. In this approach any additional requirement like ACTIONS are added after the data is loaded.

**By default kart will create a main branch so use -b master for a forced load.**

Using CLI (Kart enabled) - cd to working folder. For example c:\data\toposource

> kart init -b master topographic-data

OR..

**Note: This is aim if loading a single layer - typically fails for builk replacement**
if work flow is going to load to branch and then PR into master create a branch (reflect release version)

Make sure it is a new branch

kart init -b release64 topographic-data

THEN...

> cd topographic-data [or other target repo]

> kart remote add origin git@github.com:linz/topographic-data [or target repo for other data]

**CHECK** the remote is correct. If not correct remove - kart remote remove origin and redo last command.

> kart remote -v

**WARNINGS - if the tree_locations upload failure has not be fxed follow the tree_location load process 1st**

instruction currently in another project. To transfer.
If run - REMOVE the tree_location from the master import file.

**run the kart_import**
Note: this uses a force option on the kart push command - force clears all tables and code in the repo. Remove in check step if loading to branch.

> copy the kart_import_topodata.bat into the topographic-data folder and check settings correct - SEE: **CHECK the import BAT file has the correct settings**

DO first: Run the tree_locations manually first sometimes it works. If not the the README_TREE_LOCATIONS.md instructions. Best to do this work-around first. If it fails you may need to delete the topographic-data folder and contents and redo steps from the Kart Import instructions (this section)

Run: bat file (windows). This does a push after each load so don't hit timeout issues.

When ready - run...

@kart_import_topodata.bat

## RESET GITHUB BRANCH PROTECTION

APPLY the Branch protection rules on the master branch. Requires correct level of permissions.

# PROCESS CONTOURS

**FOLLOW THE SAME PROCESS AS MAIN TOPOGRAPHIC DATA ABOVE**

As for instructions above - check branch and schema. Data comes from the same schema as topographic-data just into its own repo.

_Use Approach 1 or Approach 2 or can clone and do 'kart data rm contour' then push (will retain history!_

**Example - use topographic-contour-data**

kart init -b master topographic-contour-data

cd topographic-contour-data

kart remote add origin git@github.com:linz/topographic-contour-data

Check all ok

> kart remote -v

Single layer so can run manually or

copy kart_import_contours.bat into topographic-contour-data

@kart_import_contours

# PROCESS PRODUCTS

See README_PRODUCTS.md for instructions on loading and processing product datasets.

# Control Files Information

CSV versions of these files are stored in the core code folder

**_LAYERS_INFO excel_**
object_name - LAMBS object name

shp_name - name of shape file no extension (building_pnt)

key - not used

theme - high level theme group

dataset - dataset (edit) level group - ArcGIS=dataset; PostGIS=not used but could be schema

classification - coded classification to use - added as field

layer_name - new table name

type - orginal geom type from LAMPS

**DATASETS_FIELDS_IMPLEMENT excel**

dataset - dataset (edit) level group [match with layers_info]

layer = layer_name [match with layers_info]
