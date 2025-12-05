import duckdb

# Connect to DuckDB and load PostGIS extension
con = duckdb.connect()
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")
con.execute("INSTALL postgres_scanner;")
con.execute("LOAD postgres_scanner;")


# Define PostGIS connection string (update with your credentials)
postgis_conn_str = "host=localhost dbname=topo50 user=postgres password=landinformation port=5432"

# Query buildings.building table
fields = "id, t50_fid, feature_type, building_use, name, status, source, source_date, capture_method, change_type, update_date, topo_id, theme, object_name, ST_GeomFromWKB(geometry)"

con.execute(f"""
    CREATE TABLE buildings AS
    SELECT {fields} FROM postgres_scan('{postgis_conn_str}', 'buildings', 'building')
""")


print("Exporting to GeoParquet...")
con.execute("""
    COPY buildings TO 'C:/Data/temp/buildings-db.parquet' (FORMAT 'PARQUET', COMPRESSION zstd, COMPRESSION_LEVEL 1, ROW_GROUP_SIZE 500)
""")
