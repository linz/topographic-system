import os
import sedona.db
import geopandas as gpd

local_app_data = os.getenv('LOCALAPPDATA')
folder = os.path.join(local_app_data, "miniconda3", "pkgs", "proj-9.6.2-h4f671f6_0", "Library", "share", "proj")
sedona.db.context.configure_proj(database_path=folder)

sd = sedona.db.connect()

df_points = sd.read_parquet(r"c:\data\temp\building_point.parquet")
df_polys = sd.read_parquet(r"c:\data\temp\building.parquet")