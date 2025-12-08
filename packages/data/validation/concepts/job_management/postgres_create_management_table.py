import psycopg

# Database connection parameters
db_params = {
    'dbname': 'topo',
    'user': 'postgres',
    'password': 'landinformation',
    'host': 'localhost',
    'port': 5432
}

schema = "editjobs"
table = f"{schema}.job_management"

# Create schema if it does not exist
with psycopg.connect(**db_params) as conn:
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    conn.commit()

    with psycopg.connect(**db_params) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    id SERIAL PRIMARY KEY,
                    guid_id UUID DEFAULT gen_random_uuid(),
                    job_name VARCHAR(50),
                    status VARCHAR(10) DEFAULT 'active',
                    notes VARCHAR(255),
                    assigned VARCHAR(100),
                    start_date DATE DEFAULT CURRENT_DATE,
                    end_date DATE,
                    geometry geometry(POLYGON, 4167)
                );
            """)
        conn.commit()