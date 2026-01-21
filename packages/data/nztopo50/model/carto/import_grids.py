import os
from sqlalchemy import create_engine, text
import geopandas as gpd  # type: ignore
import uuid


# Database connection parameters
db_params = "postgresql://postgres:landinformation@localhost:5432/topo"
path = r"C:\Data\Topo50\grids"
grid = "grid.gpkg"
grid_layer = "grid"
dms_grid = "dms_grid_3.gpkg"
grid_dms_layer = "dms_grid_3"

# Read the first grid file
grid_file = os.path.join(path, grid)
grid_gdf = gpd.read_file(grid_file, layer=grid_layer, engine="pyogrio")

# Read the second grid file
dms_grid_file = os.path.join(path, dms_grid)
dms_grid_gdf = gpd.read_file(dms_grid_file, layer=grid_dms_layer, engine="pyogrio")

# grid_gdf = grid_gdf.drop(columns=["fid"])
grid_gdf.insert(0, "topo_id", [uuid.uuid4() for _ in range(len(grid_gdf))])
print(grid_gdf.columns)

# dms_grid_gdf = dms_grid_gdf.drop(columns=["fid"])
dms_grid_gdf.insert(0, "topo_id", [uuid.uuid4() for _ in range(len(dms_grid_gdf))])
print(dms_grid_gdf.columns)

engine = create_engine(db_params)
schema = "carto"
grid_gdf.to_postgis(
    name="nz_topo50_grid",
    con=engine,
    schema=schema,
    if_exists="replace",
    index=False,
)

dms_grid_gdf.to_postgis(
    name="nz_topo50_dms_grid",
    con=engine,
    schema=schema,
    if_exists="replace",
    index=False,
)

with engine.connect() as conn:
    conn.execute(
        text(
            f"ALTER TABLE {schema}.nz_topo50_grid ALTER COLUMN topo_id SET DEFAULT gen_random_uuid()"
        )
    )
    conn.execute(
        text(
            f"ALTER TABLE {schema}.nz_topo50_dms_grid ALTER COLUMN topo_id SET DEFAULT gen_random_uuid()"
        )
    )
