import psycopg

# Database connection parameters
DB_PARAMS = {
    'dbname': 'topo',
    'user': 'postgres',
    'password': 'landinformation',
    'host': 'localhost',
    'port': 5432
}
SCHEMAS = [
    "release62",
    "qgis"
]

SCHEMAS_DATASETS = [
    "transport",
    "water",
    "land",
    "buildings",
    "names",
    "infrastructure",
    "relief",
    "mapsheets",
    "qgis"
]

def create_schemas(db_params, schemas):
    with psycopg.connect(**db_params) as conn:
        with conn.cursor() as cur:
            for schema in schemas:
                print(f"Creating schema: {schema}")
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
        conn.commit()
        conn.close()

if __name__ == "__main__":
    create_schemas(DB_PARAMS, SCHEMAS)
    #create_schemas(DB_PARAMS, SCHEMAS_DATASETS)  # latest version just goes into single schema