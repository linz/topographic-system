from sqlalchemy import create_engine, text
import geopandas as gpd  # type: ignore
import uuid


# Database connection parameters
db_params = "postgresql://postgres:landinformation@localhost:5432/topo"
path = r"C:\Data\Topo50\lds-nz-contours-topo-150k-GPKG\nz-contours-topo-150k.gpkg"
layer = "nz_contours_topo_150k"

# Read the shapefile using geopandas
gdf = gpd.read_file(path, layer=layer, engine="pyogrio")

#gdf = gdf.drop(columns=["FID"])
gdf.insert(0, "topo_id", [uuid.uuid4() for _ in range(len(gdf))])
gdf['feature_type'] = 'contour'
print(gdf.columns)

engine = create_engine(db_params)
schema = "release64"

if_exists_option = "replace"

chunk_size = len(gdf) // 10
chunks = [gdf.iloc[i:i + chunk_size] for i in range(0, len(gdf), chunk_size)]

for i, chunk in enumerate(chunks):
    print(f"Loading chunk {i + 1} of {len(chunks)}...")
    chunk.to_postgis(
        name="contour",
        con=engine,
        schema=schema,
        if_exists="append" if i > 0 else if_exists_option,
        index=False,
    )


with engine.connect() as conn:
    conn.execute(
        text(f"""
            ALTER TABLE {schema}.contour ADD PRIMARY KEY (topo_id);
        """)
    )


print("Data imported successfully")
