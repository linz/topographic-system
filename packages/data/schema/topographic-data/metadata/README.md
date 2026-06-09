# Create base metadata information from various sources

This code base uses existing (from topo data dictionary) and new created metadata files and images.

The source images have been zipped into a file under metadata_source. model-images.zip needs to be unzipped into working metadata_source folder.

The output is an **HTML index and features files** with imagery. It is created in the folder call metadata and has been zipped into a file under metadata_output_html for access.

The core information is stored in json file - **metadata_info.json** - it holds descriptions and default paths to imgages etc.

In addition the fields are created a images and the theme->feature_type structure are also created as images.

The default set up is inputs c:\data\model\metadata_source  and  outputs go to metadata.


## Working Structure

The code is by defualt set to c:\data\model - this can be changed in the code.

Model/metadata
Model/metadata_source

**Key model files and how they are created...**

model/metadata_source/layers_info.csv - copied from github import

model/metadata_source/layer_descriptions.csv - manually created and maintained. High level description and groups for the layers.

model/metadata_source/metadata_info.json - created via code - create_metadata_info.py

model/metadata_source/model-diagrams
model/metadata_source/model-themes

model/metadata_sourcecatalog.db - came from topo data dictionary github. Used to get descriptions of feature_types from orginal layer names.

zipped imagery contents from topo data dictionary github - unzip to
model/metadata_source/airphotos
model/metadata_source/diagrams

## Order to create source metadata information:

> **1: create_schema_feature_themes.py** - process the layers_info to create a summary of table,theme,feature_type -> model/schema_features_theme.csv

> **2: create_model_diagrams.py** -> create schema picture png files. It uses the GKPS data source - for example data to model in c:\data\topoedit\topographic-data\topographic-data.gpkg


**needs static files above in place:**

> **3: create_metadata_info.py** -> creates the main metadata control and information json file

> **4. create_category_diagram.py** -> create a diagrams showing themes and features types.


## Finally create a html files

The **create_metadata_topographic_html.py** script generate an index.html and metadata html files and copies images to create a html based metadata lookup.




## Python Files

| File Name                                 | Description                                                                                   |
|--------------------------------------------|-----------------------------------------------------------------------------------------------|
| `create_category_diagram.py`               | Generates diagrams (SVG/PNG) for categories, themes, layers, and feature types using Graphviz. |
| `create_metadata_info.py`               | Pull metadata information together into single json file. |
| `create_model_diagrams.py` | Creates diagrams from a database (SQLite). |
| `create_metadata_topographic_html.py`                  | Utilities for extracting and documenting metadata, including descriptions and features.  Creates html files.       |            |
| `create_schema_feature_theme.py`   | Extracts information from layers_info and table, theme, feature type summary fields.                  |


