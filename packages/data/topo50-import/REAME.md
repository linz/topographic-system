# Topo50 data repo
The purpose of the repo area to to store utility scripts to explore the topo50 data layers and convert into the database model.
The data validation system and rules is also defined in the validation folder.

The model loading is now designed around using *postgress /  postgis*.


# Folders and Information

## topo50 import

**layer_info folder** 

Utility code to create base layer information - uses a list of LAMPS data objects and shapefiles to create base layer_info information.


**model folder**

The layer info spreadheet **model\src\layer_info.xlsx** defines the LAMPS objects into themes and datasets and new layer names.

The field mapping spreadsheet **model\src\dataset_fields.xlsx** lists all the fields and mappings by layer.

**Note:** At this stage all the shapefile fields are mapped into the model. And then modified after initial load. See steps.


Field names have been remapped to be consistent and follow guidance https://toitutewhenua.atlassian.net/wiki/spaces/LI/pages/1129021598/Naming+Conventions+-+Field+and+Table+Rules

**Spatial Reference July 2025**

Data is currently loaded in New Zealand Mainland - NZTM2000 EPSG:2193 as this is the source.

The model design will expect the data to be converted into New Zealand Geodetic Datum NZGD2000 (EPSG: 4167)

See: [New Zealand Mainland - NZTM2000 EPSG:2193](https://toitutewhenua.atlassian.net/wiki/spaces/LI/pages/edit-v2/1120012771)

**layer-info folder**

Contains scripts to extract useful information

**metadata**

Contains scripts to generate basic metadata about layers and features from new and existing sources.

## other folders

**validation folder**

Contains source code and config to do data validation



**tools folder**

General utility tools working with topo data.


