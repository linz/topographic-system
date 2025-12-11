import os
import sedona.db
import geopandas as gpd

local_app_data = os.getenv("LOCALAPPDATA")
folder = os.path.join(
    local_app_data,
    "miniconda3",
    "pkgs",
    "proj-9.6.2-h4f671f6_0",
    "Library",
    "share",
    "proj",
)
sedona.db.context.configure_proj(database_path=folder)

sd = sedona.db.connect()

data_path = r"c:\data\topoedit\topographic-data\topographic-data.gpkg"

# gdf_points = gpd.read_file(data_path, layer="building_point")
gdf_polys = gpd.read_file(data_path, layer="building")
# gdf_polys = gpd.read_file(data_path, layer="vegetation")

# df_points = sd.create_data_frame(gdf_points)
df_polys = sd.create_data_frame(gdf_polys)

# df_points.to_view("building_points")
df_polys.to_view("building_polys")


# df_points.show()
# df_polys.show()

# print(type(df_points))

# query = "SELECT * FROM building_points"

# df = sd.sql(query).execute()

query = """
    SELECT p.topo_id, p.geometry
    FROM building_points p
    JOIN building_polys poly
    ON ST_Contains(poly.geometry, p.geometry)
"""

query = """
    SELECT DISTINCT a.*
    FROM building_polys a
    JOIN building_polys b
    ON MBRIntersects(a.geom, b.geom)
    WHERE ST_Intersects(a.geom, b.geom)
    AND a.topo_id != b.topo_id;
    """

query = """
    SELECT a.topo_id, a.geometry
    FROM building_polys a
    JOIN building_polys b
    ON ST_Intersects(a.geometry, b.geometry)
    AND a.topo_id != b.topo_id;
    """
df = sd.sql(query)  # .execute()
df.show()
table = df.to_arrow_table()
# view = df.to_view("test_view")
gdf = df.to_pandas("geometry")
df.to_parquet(r"c:\temp\test.parquet")
