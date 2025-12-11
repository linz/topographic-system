import os
from sqlalchemy import create_engine, text
import geopandas as gpd # type: ignore
import uuid


# Database connection parameters
db_params = "postgresql://postgres:landinformation@localhost:5432/topo"
path = r"C:\Data\Topo50\Topo50_carto_text_2020_09"
file = "linz_carto_text_2020_09.shp"

# shp file fail to load some uft8 issue - convert to geojson first using UI (Pro/QGIS)
file = "linz_carto_tex_FeaturesToJSO.geojson"

input_file = os.path.join(path, file)

# Read the shapefile using geopandas
gdf = gpd.read_file(input_file, engine="pyogrio")


#  'full_text': string
#  'text_bend': int
#  'text_char_spacing_distance': float
#  'text_colour': int
#  'text_font': string
#  'text_height': float
#  'text_placement': int
#  'text_size_type': int
#  'text_stretch_length': float
#  'text_string': string
#  'text_word_spacing_distance': float

# Rename shape fields as needed
gdf = gdf.rename(
    columns={
        "t_c_s_d": "text_char_spacing_distance",
        "text_colou": "text_colour",
        "text_heigh": "text_height",
        "text_place": "text_placement",
        "txt_size_t": "text_size_type",
        "txt_s_l": "text_stretch_length",
        "text_strin": "text_string",
        "t_w_s_d": "text_word_spacing_distance",
    }
)

gdf = gdf.drop(columns=["FID"])
gdf.insert(0, "topo_id", [uuid.uuid4() for _ in range(len(gdf))])
print(gdf.columns)

engine = create_engine(db_params)
schema = "carto"
gdf.to_postgis(
    name="nz_topo50_carto_text",
    con=engine,
    schema=schema,
    if_exists="replace",
    index=False,
)
with engine.connect() as conn:
    conn.execute(
        text(f"""
            ALTER TABLE {schema}.nz_topo50_carto_text ADD PRIMARY KEY (topo_id);
        """)
    )


print("Data imported successfully")
