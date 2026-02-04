# Topo50 data repo
The purpose of the repo area to to store utility scripts to explore the topo50 data layers and convert into the database model.
The data validation system and rules is also defined in the validation folder.

The model loading is now designed around using *postgres /  postgis*.
It is then loaded in kart geopackage from the postgres database.


# Folders and Information

## nztopo50 


**model folder**

The layer info spreadheet **model\src\layer_info.xlsx** defines the LAMPS objects into themes and datasets and new layer names.

The field mapping spreadsheet **model\src\dataset_fields.xlsx** lists all the fields and mappings by layer.

**Note:** At this stage all the shapefile fields are mapped into the model. And then modified after initial load. See steps.

The main loader code is run manually and found in **"src"** folder.
The carto loader is a mix of specfic loader code and copy some layers from th topo load. source in **carto** folder.
The **export** folder holds sample data to export data to parquet
The **metadata** folder holds information about the topo data generated from topo dictionary and samples and manual information. Provides a sample HTML and basic powerpoint file.


Field names have been remapped to be consistent and follow guidance https://toitutewhenua.atlassian.net/wiki/spaces/LI/pages/1129021598/Naming+Conventions+-+Field+and+Table+Rules

**Spatial Reference Dec 2025**

Data is currently sourced in New Zealand Mainland - NZTM2000 EPSG:2193 as this is the source.

The model design transforms the data to be converted into New Zealand Geodetic Datum NZGD2000 (EPSG: 4167)

See: [New Zealand Mainland - NZTM2000 EPSG:2193](https://toitutewhenua.atlassian.net/wiki/spaces/LI/pages/edit-v2/1120012771)


**validation folder**

Contains source code and config to do data validation

## other folders


**tools folder - not in main repo**

General utility tools working with topo data.

**layer_info folder - not in main repo** 

This is currently maintain on a seperate repo for temporary code.

Contains scripts to extract useful information.

Utility code to create base layer information - uses a list of LAMPS data objects and shapefiles to create base layer_info information.




