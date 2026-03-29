## Prerequisite

The process use POSTGRES/POSTGIS database to host data. This makes it easy to check.

Kart installation is required.

Access to GitHub repos is required.

It requires local sources of data. The default location is under c:\data\topo50.

See the README_SOURCES.md file for further information.

The code is based on Python using the dependencies listed in `requirements.txt`. Install dependencies with:

```bash
pip install -r requirements.txt
```

### Required Software:
- **PostgreSQL/PostGIS** - Spatial database for hosting topographic data
- **Python 3.8+** - Core scripting environment
- **GDAL/OGR** - Geospatial data abstraction library

### Python Dependencies:
See `requirements.txt` for complete list of required packages including:
- Database: psycopg, sqlalchemy
- Geospatial: geopandas, pyogrio, shapely, pyproj
- Data processing: pandas, numpy, pyarrow
- Visualization: graphviz, python-pptxvarious libraries - the following covers all the code under model folder.

**Standard Library Imports**

os - Operating system interface

glob - Unix style pathname pattern expansion

json - JSON encoder and decoder

sqlite3 - SQLite database interface

subprocess - Subprocess management

shutil - High-level file operations

time - Time access and conversions

uuid - UUID objects according to RFC 4122

typing.Any - Type hints


**Database & Data Processing**

psycopg - PostgreSQL database adapter

pandas as pd - Data analysis and manipulation tools

sqlalchemy.create_engine - SQL toolkit and ORM

sqlalchemy.text - SQL expression elements

duckdb - In-memory analytical database

Geospatial Libra


**Geospatial Libraries**

geopandas as gpd - Geographic data analysis

pyogrio.read_info - Fast vector I/O

pyogrio.write_dataframe - Fast vector I/O

pyogrio.list_layers - Vector layer listing

pyproj - Cartographic projections and coordinate transformations

shapely.force_2d - Geometric operations (Shapely 2.x)

fiona - Vector data I/O

osgeo.ogr - GDAL/OGR spatial data library


**Data Format Libraries**

pyarrow as pa - Apache Arrow columnar format

pyarrow.parquet as pq - Parquet file format support

numpy as np - Numerical computing

**Visualization & Documentation**

graphviz.Digraph - Graph visualization

pptx.Presentation - PowerPoint presentation creation

pptx.util.Inches - PowerPoint utilities

pptx.util.Pt - PowerPoint utilities

Local/Internal Imports

db_common_connection.DBTables - Local database connection module