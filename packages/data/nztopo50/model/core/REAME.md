
# Create Model

The topo50 model can be created dynamically using these scripts and control spreadsheets

These scripts primarily support the postgis database version although the load script is currently generic.
For **geodatabase** scripts - see the topo50data_arcgis repo (this may be out-of-date)

The layer info spreadheet **model\layer_info.xlsx** defines the LAMPS objects into themes and datasets and new layer names.

The field mapping spreadsheet **model\dataset_fields.xlsx** lists all the fields and mappings by layer.
Setting up data model and loading data into Postgres

**This process is currently manually run.
The default location for the configuration files is c:\data\model**

To help identify change the 2 configuration files - dataset_fields.xlsx and layers_info.xlsx are expected to be in this folder. They have been saved as CSV files in the repo.

The code has a default database "topo" and password. If different ones used these will need to be changed.

## Process

## The following PRE-PROCESS STEPS are required

Replace lds export with LAMPS direct export

**road_cl.shp** - this is a direct export from LAMPS as it contains additional fields

If road_cl.cpg exists - delete or rename - read has issue if it exists.

**islands_poly.shp** - this needs the additional field (location) and calculation of offshore (1) or inland island (0) - added using the pre_processing_steps.py script. The sea_coastline poly shapefile create from coastline and outer box

## MAIN Automated Steps

*Step 1: Create schemas in PostGIS*

    Note: Typically done manually. Example code: postgis_create_schemas.py

*Step 2: Create Model in PostGIS* - Note: option to DROP tables available. command_option = 'drop_tables'. Check database after to ensure any changes didn't delete a table. Delete manually if that is the case.

    Run script manually - postgis_create_model.py
    set the command_option = 'create_model'
    check model_fields_file is pointing to the dataset_fields.xlsx file
    check the data load schema name - schema_name = "release62"
    by default - primary_key_type = 'uuid'


*Step 3: Load Shapefiles into PostGIS*

    Run script manually - load_shp_to_themes.py
    pyproj (project database) - had issue locating this after qgis installed. This can be commented out if not an issue.
    check layer_info_file is pointing to the layers_info.xlsx file
    set the release value - release=release62 - This should be the same as the schema in create model step and folder name containing data to load or hard code
    set data_folder location - Expects LDS source shapefiles exported from LAMPS. Needs to confirm/adapt if other approach used.


*Step 4: Create Indexes in PostGIS*
    Run script manually - postgis_create_model.py
    set the command_option = 'create_indexes'

*Step 5: Add additional feature metadata fields and Reproject to EPSG:4167 and restructre field order, update nulls etc in PostGIS*

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

*Note: Apply Constraints* -while this is code in database_rules - the approach will be to apply this via QGIS


## Kart Import and Push
kart init -b master topographic-data

cd  topographic-data

kart remote add origin git@github.com:linz/topographic-data  [or target repo for other data]

**warning - if the tree_locations upload failure has not be fxed follow the tree_location load process 1st**
instruction currently in another project. To transfer.
If run - REMOVE the tree_location from the master import file.

**run the kart_import**
Note: this uses a force option on the kart push command - that clear all visible tables in the repo

copy the kart_import_topodata.bat into the topographic-data folder and run

@kart_import_topodata.bat

**contours**
kart init -b master topographic-contour-data

cd  topographic-data

@kart_import_contours

## Control Files

***LAYERS_INFO excel***
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