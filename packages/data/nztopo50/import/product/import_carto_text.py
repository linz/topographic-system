import os
from sqlalchemy import create_engine, text, String, Integer, Float
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
import geopandas as gpd
import pandas as pd
import uuid

# source = 'windows-json'
# source = 'windows-shp'
source = 'windows-gpkg'
# source = 'aws-gpkg'
# Database connection parameters
db_params = "postgresql+psycopg://postgres:landinformation@localhost:5432/topo"

if 'windows' in source:
    # path = r"C:\Data\Topo50\Topo50_carto_text_2020_09"
    path = r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data"
else:
    path = "s3://tbc/source/lamps/linz_carto_text_2020_09"

if source == 'windows-json':
    # shp file fail to load some uft8 issue - convert to geojson first using UI (Pro/QGIS)
    file = "linz_carto_tex_FeaturesToJSO.geojson"
elif source == 'windows-shp':
    file = "linz_carto_text_2020_09.shp"
else:
    file = "topographic-source-data.gpkg"
    layer = "linz_carto_text"


input_file = os.path.join(path, file)

# Read the shapefile using geopandas
if 'gpkg' in source:
    gdf = gpd.read_file(input_file, layer=layer, engine="pyogrio")
else:
    gdf = gpd.read_file(input_file, engine="pyogrio")

#  'full_text': string 100
#  'text_bend': int
#  'text_char_spacing_distance': float
#  'text_colour': int
#  'text_font': string 50
#  'text_height': float (round 4 decimal places)
#  'text_orientation': float (round 2 decimal places)
#  'text_placement': int
#  'text_size_type': int
#  'text_stretch_length': float
#  'text_string': string 100
#  'text_word_spacing_distance': float

# Rename shape fields as needed
gdf = gdf.rename(
    columns={
        "t_c_s_d": "text_char_spacing_distance",
        "text_colou": "text_colour",
        "text_heigh": "text_height",
        "text_orien": "text_orientation",
        "text_place": "text_placement",
        "txt_size_t": "text_size_type",
        "txt_s_l": "text_stretch_length",
        "text_strin": "text_string",
        "t_w_s_d": "text_word_spacing_distance",
    }
)

if "FID" in gdf.columns:
    gdf = gdf.drop(columns=["FID"])

# Coerce known carto fields into stable numeric/string types before DB load.
string_columns = ["full_text", "text_font", "text_string"]
int_columns = ["text_bend", "text_colour", "text_placement", "text_size_type"]
float_columns = ["text_char_spacing_distance", "text_stretch_length", "text_word_spacing_distance"]

for col in string_columns:
    if col in gdf.columns:
        gdf[col] = gdf[col].astype("string")

for col in int_columns:
    if col in gdf.columns:
        gdf[col] = pd.to_numeric(gdf[col], errors="coerce").astype("Int32")

for col in float_columns:
    if col in gdf.columns:
        gdf[col] = pd.to_numeric(gdf[col], errors="coerce")

if "text_height" in gdf.columns:
    gdf["text_height"] = pd.to_numeric(gdf["text_height"], errors="coerce").round(4)

if "text_orientation" in gdf.columns:
    gdf["text_orientation"] = pd.to_numeric(
        gdf["text_orientation"], errors="coerce"
    ).round(2)

# Adjust schema for column widths
schema = "carto"

gdf.insert(0, "id", [uuid.uuid4() for _ in range(len(gdf))])
print(gdf.columns)

dtype_mapping = {
    'id': PG_UUID(as_uuid=True),
    'full_text': String(100),
    'text_bend': Integer(),
    'text_char_spacing_distance': Float(),
    'text_colour': Integer(),
    'text_font': String(50),
    'text_height': Float(),
    'text_orientation': Float(),
    'text_placement': Integer(),
    'text_size_type': Integer(),
    'text_stretch_length': Float(),
    'text_string': String(100),
    'text_word_spacing_distance': Float(),
}

# Write GeoDataFrame to PostGIS with explicit column types
engine = create_engine(db_params)
gdf.to_postgis(
    name="nz_topo50_carto_text",
    con=engine,
    schema=schema,
    if_exists="replace",
    index=False,
    dtype=dtype_mapping
)

with engine.connect() as conn:
    conn.execute(
        text(f"""
            ALTER TABLE {schema}.nz_topo50_carto_text ADD PRIMARY KEY (id);
        """)
    )


print("Data imported successfully")
