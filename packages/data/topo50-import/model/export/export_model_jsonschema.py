import psycopg2
import json
from model.system.db_common import DBTables

# PostgreSQL to JSON Schema type mapping
type_mapping = {
    'integer': 'integer',
    'bigint': 'integer',
    'smallint': 'integer',
    'serial': 'integer',
    'bigserial': 'integer',
    'real': 'number',
    'double precision': 'number',
    'numeric': 'number',
    'boolean': 'boolean',
    'text': 'string',
    'varchar': 'string',
    'char': 'string',
    'date': 'string',
    'timestamp': 'string',
    'timestamp without time zone': 'string',
    'timestamp with time zone': 'string',
    'USER-DEFINED': 'geometry',  # Custom types, adjust as needed
    # Add more as needed
}

def get_json_schema(table_name, conn):

    cur = conn.cursor()

    cur.execute(f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = %s;
    """, (table_name,))

    columns = cur.fetchall()
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": table_name,
        "type": "object",
        "properties": {},
        "required": []
    }

    for column_name, data_type, is_nullable in columns:
        json_type = type_mapping.get(data_type, 'string')  # default to string
        schema["properties"][column_name] = {"type": json_type}
        if is_nullable == 'NO':
            schema["required"].append(column_name)

    cur.close()
    return schema


if __name__ == "__main__":
    DB_PARAMS = {
        'dbname': 'topo',
        'user': 'postgres',
        'password': 'landinformation',
        'host': 'localhost',
        'port': 5432
    }
    dbtables = DBTables(DB_PARAMS)
    conn = dbtables.get_connection()

    schema_tables = dbtables.list_schema_tables()
    for schema, tables in schema_tables.items():
        for table_name in tables:

            json_schema = get_json_schema(table_name, conn)

            # Save to file
            with open(f"c:\\temp\\json_schema\\{table_name}_schema.json", "w") as f:
                json.dump(json_schema, f, indent=2)

    conn.close()