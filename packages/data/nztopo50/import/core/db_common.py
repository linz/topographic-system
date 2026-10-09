"""Common PostgreSQL table helpers for the NZTopo50 import workflow.

This module provides a small utility class for opening a database connection
and running common schema/table/column operations used during data imports.
"""

import psycopg


class DBTables:
    """Utility wrapper around a psycopg connection for table maintenance tasks.

    Args:
        db_params: Keyword arguments passed directly to ``psycopg.connect``.
    """

    def __init__(self, db_params):
        """Create a DBTables instance and establish an initial connection."""
        self.db_params = db_params
        self.conn = None
        self.connect()

    def connect(self):
        """Open a database connection if one is not currently available."""
        if self.conn is None or self.conn.closed:
            self.conn = psycopg.connect(**self.db_params)

    def close(self):
        """Close the current database connection if it is open."""
        if self.conn and not self.conn.closed:
            self.conn.close()

    def get_connection(self):
        """Return an open connection, reconnecting first if required."""
        if self.conn is None or self.conn.closed:
            self.connect()
        return self.conn

    def list_schema_tables(self):
        """List non-system schemas and their tables.

        Returns:
            dict[str, list[str]]: Mapping of schema name to table names.
        """
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
        """Return the schema row for a table name from information_schema.

        Args:
            table: Table name to look up.

        Returns:
            tuple | None: First row from the query, or ``None`` if not found.
        """
        self.connect()
        query = """
            SELECT table_schema FROM information_schema.tables
            WHERE table_name = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (table,))
            return cur.fetchone()

    def table_exists(self, schema, table):
        """Check whether a table exists in the given schema."""
        self.connect()
        query = """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (schema, table))
            return cur.fetchone() is not None

    def column_exists(self, schema, table, column_name):
        """Check whether a column exists in the given schema/table."""
        self.connect()
        query = """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (schema, table, column_name))
            return cur.fetchone() is not None

    def add_column(self, table_name, column_name, data_type):
        """Add a column to a table and commit the schema change.

        If ``data_type`` already contains ``DEFAULT``, it is used as-is;
        otherwise, ``DEFAULT NULL`` is appended.
        """
        self.connect()
        with self.conn.cursor() as cur:
            # Check if data_type contains a DEFAULT value
            if "DEFAULT" in data_type:
                query = (
                    f'ALTER TABLE {table_name} ADD COLUMN "{column_name}" {data_type};'
                )
            else:
                query = f'ALTER TABLE {table_name} ADD COLUMN "{column_name}" {data_type} DEFAULT NULL;'
            cur.execute(query)
            self.conn.commit()
            print(
                f"Added column '{column_name}' to table '{table_name}' with data type '{data_type}'"
            )

    def update_null_column_with_default(
        self, schema, table, column_name, default_value
    ):
        """Set a column to a default value for all rows in a table."""
        self.connect()
        with self.conn.cursor() as cur:
            update_query = (
                f"UPDATE {schema}.{table} SET {column_name} = {default_value};"
            )
            cur.execute(update_query)
            self.conn.commit()
            print(
                f"Updated '{column_name}' in '{schema}.{table}' with default value '{default_value}'"
            )

    def rename_columns(self, schema, table, old_column_name, new_column_name):
        """Rename a column when it exists, then commit the change."""
        self.connect()
        with self.conn.cursor() as cur:
            if self.column_exists(schema, table, old_column_name):
                rename_query = f"ALTER TABLE {schema}.{table} RENAME COLUMN {old_column_name} TO {new_column_name};"
                cur.execute(rename_query)
                self.conn.commit()
                print(
                    f"Renamed column '{old_column_name}' to '{new_column_name}' in table '{schema}.{table}'"
                )
            else:
                print(
                    f"Column '{old_column_name}' does not exist in table '{schema}.{table}'"
                )

    def set_base_column_and_drop_column(
        self, schema, table, base_column_name, drop_column_name
    ):
        """Copy values into a base column and drop the source column.

        Rows are updated only when the source column is not null.
        """
        self.connect()
        with self.conn.cursor() as cur:
            if self.column_exists(schema, table, base_column_name):
                # Set the new column as base
                set_base_query = f"UPDATE {schema}.{table} SET {base_column_name} = {drop_column_name} WHERE {drop_column_name} IS NOT NULL;"
                cur.execute(set_base_query)
                # Drop the old column
                drop_query = (
                    f"ALTER TABLE {schema}.{table} DROP COLUMN {drop_column_name};"
                )
                cur.execute(drop_query)
                self.conn.commit()
                print(
                    f"Set '{base_column_name}' as base and dropped '{drop_column_name}' in table '{schema}.{table}'"
                )
            else:
                print(
                    f"Column '{base_column_name}' does not exist in table '{schema}.{table}'"
                )
