# Create base metadata information from various sources

This code base uses existing (from topo data dictionary) and new created metadata files and images.

The sources have been zipped into a file under metadata_source
The basic output (HTML and Powerpoint) have been zipped into a file under metadata

The core information is stored in json file - **metadata_info.json** - it holds descriptions and default paths to imgages etc.

The default set up is c:\data\model\metadata  and  source_metadata

## Python Files

| File Name                                 | Description                                                                                   |
|--------------------------------------------|-----------------------------------------------------------------------------------------------|
| `create_category_diagram.py`               | Generates diagrams (SVG/PNG) for categories, themes, layers, and feature types using Graphviz. |
| `create_eralchemy_diagram_bat_postgres.py` | Creates a .bat file to run eralchemy for ER diagrams from a database (mainly for Postgres/SQLite). |
| `create_metadata_docs.py`                  | Utilities for extracting and documenting metadata, including descriptions and features.  Creates json and html files.       |
| `create_powerpoint_from_metadata.py`       | Generates PowerPoint presentations from metadata, including images and summaries.               |
| `import_description_from_metadata_db.py`   | Example code for querying object descriptions from a SQLite metadata database.                  |
