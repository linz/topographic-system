import duckdb


# Connect to DuckDB and load PostGIS extension
con = duckdb.connect()
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")
con.execute("INSTALL postgres_scanner;")
con.execute("LOAD postgres_scanner;")

field_order = ['a','b','c','d']
in_order = ['c','b','a','z','d']
# Reorder in_order to match the order in field_order, keeping only items present in both
reordered = [f for f in field_order if f in in_order]
print(reordered)
# Reorder in_order so that items in field_order come first (in field_order order), then the rest of in_order
reordered_full = [f for f in field_order if f in in_order] + [f for f in in_order if f not in field_order]
print(reordered_full)

# Define PostGIS connection string (update with your credentials)
postgis_conn_str = "host=localhost dbname=topo50 user=postgres password=landinformation port=5432"

# Query buildings.building table
query = "id, macronated, building_use, name_ascii, t50_fid, name, status, theme, feature_type, object_name, shape_area, " \
         "geometry, " \
         "source, source_date, capture_method, change_type, update_date, topo_id"

query = "id, ST_AsText(ST_GeomFromWKB(geometry) as geometry"
query = "id, ST_Transform(ST_GeomFromWKB(geometry),'EPSG:4167', 'EPSG:4167') as geometry"

#ST_Transform(pickup_point, 'EPSG:4326', 'ESRI:102718')
#query = "id, geometry"



#info = con.execute(f"""
#    SELECT id,ST_GeomFromWKB(GEOMETRY) FROM postgres_scan('{postgis_conn_str}', 'buildings', 'building_4167')
#""")

#print(info.fetchall())


con.execute(f"""
    CREATE TABLE buildings AS
    SELECT {query} FROM postgres_scan('{postgis_conn_str}', 'buildings', 'building_4167')
""")

#SELECT decode(value) as col FROM parquet_kv_metadata('http://github.dev/opengeospatial/geoparquet/blob/main/examples/example.parquet');



describe = con.execute("""
    DESCRIBE buildings 
""")
print(describe.fetchall())

info = con.execute("""
    select geometry from buildings 
""")
#print(info.fetchall())

# Export to GeoParquet
option = 1

if option == 1:
    print("Exporting to GeoParquet...")
    con.execute("""
        COPY buildings TO 'C:/Data/temp/buildings-4167b.parquet' (FORMAT 'PARQUET', COMPRESSION zstd, COMPRESSION_LEVEL 1, ROW_GROUP_SIZE 500)
    """)
elif option == 2:
    print("Exporting to GPKG...")
    con.execute("""
        COPY buildings TO 'C:/Data/temp/buildings2.gpkg' (FORMAT 'GDAL', DRIVER 'GPKG',  LAYER_CREATION_OPTIONS 'WRITE_BBOX=YES')
    """)
else:
    print("Exporting to GDAL Parquet...")
    con.execute("""
        COPY buildings TO 'C:/Data/temp/buildings3.parquet' (FORMAT 'GDAL', DRIVER 'PARQUET', LAYER_CREATION_OPTIONS 'WRITE_BBOX=YES')
    """)


#FORMAT gdal, DRIVER 'GeoJSON', LAYER_CREATION_OPTIONS 'WRITE_BBOX=YES'