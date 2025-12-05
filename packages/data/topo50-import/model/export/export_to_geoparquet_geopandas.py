import os
import psycopg
import geopandas as gpd
from sqlalchemy import create_engine

class Database:
    def __init__(self, db_params):
        self.db_params = db_params
        self.conn = None
        self.connect()

    def connect(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg.connect(**self.db_params)

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()

    def list_schema_tables(self, schema_name):
        self.connect()

        query = f"""
        SELECT table_schema, table_name FROM information_schema.tables
        WHERE table_schema = '{schema_name}'
        ORDER BY table_schema, table_name
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            schema_tables = {}
            for schema, table in rows:
                if schema not in schema_tables:
                    schema_tables[schema] = []
                schema_tables[schema].append(table)
        return schema_tables

    def list_columns(self, schema, table):
        self.connect()

        query = f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{table}' 
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            columns = [row[0] for row in cur.fetchall()]
            return columns

if __name__ == "__main__":
    export_topo_data = False
    export_carto_data = True

    DB_PARAMS = {
        'dbname': 'topo',
        'user': 'postgres',
        'password': 'landinformation',
        'host': 'localhost',
        'port': 5432
    }
    db_connection_url = "postgresql://postgres:landinformation@localhost:5432/topo"
    schema = 'release62'
    release_date = "2025-02-05"
    export_folder = os.path.join("C:/Data/temp/", release_date)
    if not os.path.exists(export_folder):
        os.makedirs(export_folder)

    database = Database(DB_PARAMS)
    sqlcon = create_engine(db_connection_url)
    tables = database.list_schema_tables(schema)
    non_spatial_tables = ['collections']

    if  export_topo_data:
        for table in tables.get(schema, []):
            print(f"Table: {table}")
            columns = database.list_columns(schema, table)
            fields = ", ".join(columns)
            fields = fields.replace("topo_id", "topo_id::text")
            print(fields)

            sql = f"SELECT {fields} FROM {schema}.{table}"
            parquet_file = os.path.join(export_folder, f"{table}.parquet")
            print(f"Exporting to {parquet_file}")

            if table not in non_spatial_tables:
                df = gpd.GeoDataFrame.from_postgis(sql, sqlcon)
                gdf = gpd.GeoDataFrame(df, geometry='geom', crs='EPSG:4167')
                gdf.to_parquet(parquet_file, engine='pyarrow', compression='zstd', compression_level=3, write_covering_bbox=True, row_group_size=50000)
            else:
                df = gpd.pd.read_sql(sql, sqlcon)
                df.to_parquet(parquet_file, engine='pyarrow', compression='zstd', compression_level=3, row_group_size=50000)

    if export_carto_data:
        # export the carto dataset
        schema = 'carto'
        table = 'topo50_carto_text'
        print(f"Table: {table}")
        columns = database.list_columns(schema, table)
        fields = ", ".join(columns)
        fields = fields.replace("topo_id", "topo_id::text")
        print(fields)

        sql = f"SELECT {fields} FROM carto.carto_text"
        parquet_file = os.path.join(export_folder, "carto_text.parquet")
        print(f"Exporting to {parquet_file}")

        df = gpd.GeoDataFrame.from_postgis(sql, sqlcon, geom_col='geometry')
        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4167')
        gdf.to_parquet(parquet_file, engine='pyarrow', compression='zstd', compression_level=3, write_covering_bbox=True, row_group_size=50000)
