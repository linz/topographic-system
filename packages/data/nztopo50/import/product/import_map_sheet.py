import os
from numbers import Real
from sqlalchemy import create_engine, text, String, Integer, Date
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, DOUBLE_PRECISION
import geopandas as gpd
import pandas as pd
import uuid

final_drop_fields = False
# source = 'windows-shp'
source = 'windows-gpkg'
# source = 'aws-gpkg'
# Database connection parameters
db_params = "postgresql+psycopg://postgres:landinformation@localhost:5432/topo"

if 'windows' in source:
    # path = r"C:\Data\Topo50\Topo50_carto_text_2020_09"
    path = r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data"
else:
    path = "s3://tbc/source/lamps/linz_map_sheet"  # GUESS

if source == 'windows-shp':
    file = "linz_carto_text_2020_09.shp"
else:
    file = "topographic-source-data.gpkg"
    layer = "linz_map_sheet"

carto_schema="carto"
release_schema="release66"
topo_id_name="id"

def top_left_from_geometry(geom):
    if geom is None or geom.is_empty:
        return (pd.NA, pd.NA)

    xs = []
    ys = []

    def walk_coords(node):
        if isinstance(node, (list, tuple)):
            if len(node) >= 2 and isinstance(node[0], Real) and isinstance(node[1], Real):
                xs.append(float(node[0]))
                ys.append(float(node[1]))
            else:
                for child in node:
                    walk_coords(child)

    walk_coords(geom.__geo_interface__.get("coordinates"))
    if not xs or not ys:
        return (pd.NA, pd.NA)

    return (min(xs), max(ys))

input_file = os.path.join(path, file)

# Read the shapefile using geopandas
if 'gpkg' in source:
    gdf = gpd.read_file(input_file, layer=layer, engine="pyogrio")
else:
    gdf = gpd.read_file(input_file, engine="pyogrio")



# Rename shape fields as needed
gdf = gdf.rename(
    columns={
        "ex_name": "example_name",
        "ex_class": "example_class",
        "sheet_code": "sheet_code",
        "sheet_name": "sheet_name",
        "edition": "edition",
        "t50_fid": "t50_fid",
    }
)

if "FID" in gdf.columns:
    gdf = gdf.drop(columns=["FID"])

# Coerce known carto fields into stable numeric/string types before DB load.
string_columns = ["example_name", "example_class", "sheet_code", "sheet_name", "edition"]
#int_columns = ["x_origin", "y_origin"]
int64_columns = ["t50_fid"]


for col in string_columns:
    if col in gdf.columns:
        gdf[col] = gdf[col].astype("string")

#for col in int_columns:
#    if col in gdf.columns:
#        gdf[col] = pd.to_numeric(gdf[col], errors="coerce").astype("Int32")

for col in int64_columns:
    if col in gdf.columns:
        gdf[col] = pd.to_numeric(gdf[col], errors="coerce").astype("Int64")


# Adjust schema for column widths
schema = "carto"

gdf.insert(0, "id", [uuid.uuid4() for _ in range(len(gdf))])

# Add required output fields for map sheet load.
gdf["type"] = "nztopo50_map_sheet"

origins = gdf.geometry.apply(top_left_from_geometry)
origins_df = origins.apply(pd.Series)
gdf["x_origin"] = pd.to_numeric(origins_df[0], errors="coerce").round(0).astype("Int32")
gdf["y_origin"] = pd.to_numeric(origins_df[1], errors="coerce").round(0).astype("Int32")

if "example_point_id" not in gdf.columns:
    gdf["example_point_id"] = pd.NA

# Example edition format: "Edition 1.01 Published 2024"
if "edition" in gdf.columns:
    edition_series = gdf["edition"].astype("string")
    gdf["published_version"] = edition_series.str.extract(
        r"Edition\s+([0-9]+(?:\.[0-9]+)?)",
        expand=False,
    )

    published_year = pd.to_numeric(
        edition_series.str.extract(r"Published\s+([0-9]{4})", expand=False),
        errors="coerce",
    ).astype("Int64")
    published_at = pd.to_datetime(published_year.astype("string") + "-01-01", errors="coerce")
    gdf["published_at"] = published_at.dt.date
else:
    gdf["published_version"] = pd.NA
    gdf["published_at"] = pd.NA

gdf["updated_at"] = gdf["published_at"]

print(gdf.columns)

dtype_mapping = {
    'id': PG_UUID(as_uuid=True),
    't50_fid': Integer(),
    'type': String(50),
    'example_name': String(150),
    'example_class': String(30),
    'sheet_code': String(21),
    'sheet_name': String(50),
    'edition': String(30),
    'x_origin': DOUBLE_PRECISION(),
    'y_origin': DOUBLE_PRECISION(),
    'example_point_id': String(40),
    'published_version': String(25),
    'published_at': Date(),
    'updated_at': Date(),
}

# Order fields by dtype mapping keys and keep geometry as the final column.
ordered_columns = [col for col in dtype_mapping.keys() if col in gdf.columns]
geometry_column = gdf.geometry.name
if geometry_column in gdf.columns:
    ordered_columns.append(geometry_column)
gdf = gdf[ordered_columns]


def update_example_point_ids(conn, carto_schema="carto", release_schema="release66", topo_id_name = "id"):
    conn.execute(
        text(f"""
            UPDATE {carto_schema}.nztopo50_map_sheet ms
            SET example_point_id = tp.{topo_id_name}
            FROM {release_schema}.trig_point tp
            WHERE tp.code = ms.example_name
              AND ms.example_class = 'trig_pnt';
        """)
    )

    conn.execute(
        text(f"""
            UPDATE {carto_schema}.nztopo50_map_sheet ms
            SET example_point_id = gn.{topo_id_name}
            FROM {release_schema}.geographic_name gn
            WHERE gn.name = ms.example_name
              AND ms.example_class = 'geographic_name';
        """)
    )

def data_fixes(conn, carto_schema="carto"):
    conn.execute(
        text(f"""
            UPDATE {carto_schema}.nztopo50_map_sheet ms
            SET example_name = regexp_replace(example_name, '^Mt\\s+', 'Mount ')
            WHERE example_point_id IS NULL
              AND example_name ~ '^Mt\\s+';
        """)

    )
    conn.execute(
        text(f"""
            UPDATE {carto_schema}.nztopo50_map_sheet ms
            SET example_name = CASE
                WHEN example_name = 'A0TR' THEN 'A0U2'
                WHEN example_name = 'AP8Y' THEN 'A4UX'
                ELSE example_name
            END
            WHERE example_name IN ('A0TR', 'AP8Y');
        """)
    )
    conn.execute(
        text(f"""
            UPDATE {carto_schema}.nztopo50_map_sheet ms
            SET example_name = CASE
                WHEN example_name = 'Putata' THEN 'Pūtata'
                WHEN example_name = 'Pohoi' THEN 'Pōhoi'
                WHEN example_name = 'Rahuimokairoa' THEN 'Rāhuimōkairoa'
                ELSE example_name
            END
            WHERE example_name IN ('Putata', 'Pohoi', 'Rahuimokairoa');
        """)
    )

def drop_post_update_columns(conn, carto_schema="carto"):
    conn.execute(
        text(f"""
            ALTER TABLE {carto_schema}.nztopo50_map_sheet
            DROP COLUMN IF EXISTS example_name,
            DROP COLUMN IF EXISTS example_class,
            DROP COLUMN IF EXISTS edition,
            DROP COLUMN IF EXISTS revised;
        """)
    )

# Write GeoDataFrame to PostGIS with explicit column types
engine = create_engine(db_params)
gdf.to_postgis(
    name="nztopo50_map_sheet",
    con=engine,
    schema=schema,
    if_exists="replace",
    index=False,
    dtype=dtype_mapping
)

with engine.begin() as conn:
    conn.execute(
        text(f"""
            ALTER TABLE {schema}.nztopo50_map_sheet ADD PRIMARY KEY (id);
        """)
    )
    update_example_point_ids(conn, carto_schema=carto_schema, release_schema=release_schema, topo_id_name=topo_id_name)

    # temp fix for missing example_point_ids
    data_fixes(conn, carto_schema=carto_schema)
    update_example_point_ids(conn, carto_schema=carto_schema, release_schema=release_schema, topo_id_name=topo_id_name)

    if final_drop_fields:
        drop_post_update_columns(conn, carto_schema=carto_schema)
        conn.commit()

    # remove 3 superfluous polygon areas not needed for map sheet product
    conn.execute(
        text(f"""
            DELETE FROM {carto_schema}.nztopo50_map_sheet
            WHERE sheet_code LIKE 'Topo%';
        """)
    )


    conn.commit()

print("Data imported successfully")


