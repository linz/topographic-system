import psycopg


class DBTables:
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

    def get_connection(self):
        if self.conn is None or self.conn.closed:
            self.connect()
        return self.conn

    def list_schema_tables(self):
        self.connect()

        query = """
        SELECT table_schema, table_name FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'ogr_system_tables', 'pg_catalog', 'public', 'qgis','audit')
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

    def table_schema(self, table):
        self.connect()
        query = """
            SELECT table_schema FROM information_schema.tables
            WHERE table_name = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (table,))
            return cur.fetchone() 
        
    def table_exists(self, schema, table):
        self.connect()
        query = """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (schema, table))
            return cur.fetchone() is not None

    def column_exists(self, schema, table, column_name):
        self.connect()
        query = """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (schema, table, column_name))
            return cur.fetchone() is not None

    def add_column(self, table_name, column_name, data_type):
        self.connect()
        with self.conn.cursor() as cur:
            # Check if data_type contains a DEFAULT value
            if "DEFAULT" in data_type:
                query = f'ALTER TABLE {table_name} ADD COLUMN "{column_name}" {data_type};'
            else:
                query = f'ALTER TABLE {table_name} ADD COLUMN "{column_name}" {data_type} DEFAULT NULL;'
            cur.execute(query)
            self.conn.commit()
            print(f"Added column '{column_name}' to table '{table_name}' with data type '{data_type}'")

    def update_null_column_with_default(self, schema, table, column_name, default_value):
        self.connect()
        with self.conn.cursor() as cur:
            update_query = f'UPDATE {schema}.{table} SET {column_name} = {default_value};'
            cur.execute(update_query)
            self.conn.commit()
            print(f"Updated '{column_name}' in '{schema}.{table}' with default value '{default_value}'")

    def rename_columns(self, schema, table, old_column_name, new_column_name):
        self.connect()
        with self.conn.cursor() as cur:
            if self.column_exists(schema, table, old_column_name):
                rename_query = f'ALTER TABLE {schema}.{table} RENAME COLUMN {old_column_name} TO {new_column_name};'
                cur.execute(rename_query)
                self.conn.commit()
                print(f"Renamed column '{old_column_name}' to '{new_column_name}' in table '{schema}.{table}'")
            else:
                print(f"Column '{old_column_name}' does not exist in table '{schema}.{table}'")

    def set_base_column_and_drop_column(self, schema, table, base_column_name, drop_column_name):
        self.connect()
        with self.conn.cursor() as cur:
            if self.column_exists(schema, table, base_column_name):
                # Set the new column as base
                set_base_query = f'UPDATE {schema}.{table} SET {base_column_name} = {drop_column_name} WHERE {drop_column_name} IS NOT NULL;'
                cur.execute(set_base_query)
                # Drop the old column
                drop_query = f'ALTER TABLE {schema}.{table} DROP COLUMN {drop_column_name};'
                cur.execute(drop_query)
                self.conn.commit()
                print(f"Set '{base_column_name}' as base and dropped '{drop_column_name}' in table '{schema}.{table}'")
            else:
                print(f"Column '{base_column_name}' does not exist in table '{schema}.{table}'")

    