#!/usr/bin/env python3
"""
Standalone script for updating topo_id values from previous database releases.

This script updates topo_id values in a current schema based on matching t50_fid values
from a previous release schema. This ensures consistent identifiers across releases
when features are carried over.
"""

import psycopg

# Database connection parameters
DB_PARAMS = {
    "dbname": "topo",
    "user": "postgres", 
    "password": "landinformation",
    "host": "localhost",
    "port": 5432,
}


class TopoIdUpdater:
    """Class for updating topo_id values from previous releases."""
    
    def __init__(self, db_params):
        """Initialize the updater and open a database connection.

        Args:
            db_params: Mapping of connection arguments accepted by
                `psycopg.connect`.
        """
        self.db_params = db_params
        self.conn = None
        self.connect()

    def connect(self):
        """Establish a database connection if none is open."""
        if self.conn is None or self.conn.closed:
            self.conn = psycopg.connect(**self.db_params)

    def close(self):
        """Close the active database connection if open."""
        if self.conn and not self.conn.closed:
            self.conn.close()

    def list_schema_tables(self, schema_name):
        """List tables for the provided schema.

        Args:
            schema_name: Name of the schema to inspect.

        Returns:
            dict: Mapping of schema name to a list of table names.
        """
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

    def column_exists(self, schema, table, column_name, use_like=False):
        """Check whether a column exists in a table.

        Args:
            schema: Table schema name.
            table: Table name.
            column_name: Column name or partial name.
            use_like: If True, use SQL `LIKE` matching for `column_name`.

        Returns:
            bool: True when a matching column exists.
        """
        self.connect()
        if use_like:
            column_sql = f"like '%{column_name}%'"
        else:
            column_sql = f"= '{column_name}'"
        query = f"""
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{table}' AND column_name {column_sql}
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchone() is not None

    def table_exists(self, schema, table):
        """Check whether a table exists in a schema.

        Args:
            schema: Schema name.
            table: Table name.

        Returns:
            bool: True when the table exists.
        """
        self.connect()
        query = f"""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = '{table}'
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchone() is not None

    def update_topoid_from_previous_release(self, schema, table, previous_schema):
        """Update `topo_id` values by matching `t50_fid` with a previous release.

        Args:
            schema: Current release schema containing target rows.
            table: Table name to update.
            previous_schema: Prior release schema used as source of `topo_id`.

        Returns:
            bool: True when processing completed for the table (including cases
            where no updates were needed), otherwise False.

        Notes:
            The update only applies when both tables exist and both contain
            `t50_fid` and `topo_id` columns.
        """
        self.connect()
        
        # Check if both tables exist
        if not self.table_exists(schema, table):
            print(f"Table '{schema}.{table}' does not exist")
            return False
            
        if not self.table_exists(previous_schema, table):
            print(f"Table '{previous_schema}.{table}' does not exist")
            return False
        
        with self.conn.cursor() as cur:
            # Check if t50_fid column exists in both tables
            if (self.column_exists(schema, table, "t50_fid") and 
                self.column_exists(previous_schema, table, "t50_fid")):
                
                # Check if topo_id column exists in both tables
                if (self.column_exists(schema, table, "topo_id") and 
                    self.column_exists(previous_schema, table, "topo_id")):
                    
                    update_query = f"""
                        UPDATE {schema}.{table} AS new
                        SET topo_id = old.topo_id
                        FROM {previous_schema}.{table} AS old
                        WHERE new.t50_fid = old.t50_fid
                        AND new.topo_id IS DISTINCT FROM old.topo_id;
                    """
                    
                    try:
                        # First, get count of records that will be updated
                        count_query = f"""
                            SELECT COUNT(*) 
                            FROM {schema}.{table} AS new
                            JOIN {previous_schema}.{table} AS old ON new.t50_fid = old.t50_fid
                            WHERE new.topo_id IS DISTINCT FROM old.topo_id;
                        """
                        cur.execute(count_query)
                        update_count = cur.fetchone()[0]
                        
                        if update_count > 0:
                            # Execute the update
                            cur.execute(update_query)
                            self.conn.commit()
                            print(f"Updated {update_count} topo_id values in '{schema}.{table}' from '{previous_schema}.{table}'")
                        else:
                            print(f"No topo_id updates needed for '{schema}.{table}' (all values already match)")
                        
                        return True
                        
                    except Exception as e:
                        print(f"Error updating topo_id in '{schema}.{table}' from '{previous_schema}.{table}': {e}")
                        self.conn.rollback()
                        return False
                else:
                    print(f"'topo_id' column missing in one of the tables '{schema}.{table}' or '{previous_schema}.{table}'")
                    return False
            else:
                print(f"'t50_fid' column missing in one of the tables '{schema}.{table}' or '{previous_schema}.{table}'")
                return False

    def get_update_statistics(self, schema, previous_schema):
        """Collect potential update statistics across schema tables.

        Args:
            schema: Current release schema.
            previous_schema: Previous release schema.

        Returns:
            list[dict]: Per-table statistics including current row count,
            matching rows by `t50_fid`, and rows requiring `topo_id` updates.
        """
        self.connect()
        schema_tables = self.list_schema_tables(schema)
        
        stats = []
        for schema_name, tables in schema_tables.items():
            for table in tables:
                if self.table_exists(previous_schema, table):
                    # Check columns exist
                    if (self.column_exists(schema_name, table, "t50_fid") and 
                        self.column_exists(previous_schema, table, "t50_fid") and
                        self.column_exists(schema_name, table, "topo_id") and 
                        self.column_exists(previous_schema, table, "topo_id")):
                        
                        # Get matching records count
                        with self.conn.cursor() as cur:
                            query = f"""
                                SELECT 
                                    COUNT(*) as total_current,
                                    COUNT(old.t50_fid) as matching_records,
                                    COUNT(CASE WHEN new.topo_id IS DISTINCT FROM old.topo_id THEN 1 END) as needs_update
                                FROM {schema_name}.{table} AS new
                                LEFT JOIN {previous_schema}.{table} AS old ON new.t50_fid = old.t50_fid
                            """
                            cur.execute(query)
                            result = cur.fetchone()
                            
                            stats.append({
                                'table': table,
                                'total_current': result[0],
                                'matching_records': result[1],
                                'needs_update': result[2]
                            })
        
        return stats


def update_all_tables(current_schema, previous_schema, skip_tables=None):
    """Run `topo_id` synchronization across all tables in a schema.

    Args:
        current_schema: Target schema to update.
        previous_schema: Source schema to copy historical `topo_id` values from.
        skip_tables: Optional list of table names to skip.

    Returns:
        tuple[int, int]: `(successful_updates, failed_updates)` counts.
    """
    updater = TopoIdUpdater(DB_PARAMS)
    
    # Get list of tables to process
    schema_tables = updater.list_schema_tables(current_schema)
    
    # Track results
    successful_updates = 0
    failed_updates = 0
    failed_table_names = []
    
    # Update topo_id from previous release
    if skip_tables is None:
        skip_tables = []
    print(f"Starting topo_id updates from {previous_schema} to {current_schema}...")
    
    for schema, tables in schema_tables.items():
        for table in tables:
            if table in skip_tables:
                print(f"Skipping table: {schema}.{table}")
                continue
            print(f"\nProcessing table: {schema}.{table}")
            success = updater.update_topoid_from_previous_release(
                schema, table, previous_schema
            )
            
            if success:
                successful_updates += 1
            else:
                failed_updates += 1
                failed_table_names.append(f"{schema}.{table}")

    # Print summary
    total_tables = successful_updates + failed_updates
    print("\n=== UPDATE SUMMARY ===")
    print(f"Total tables processed: {total_tables}")
    print(f"Successful updates: {successful_updates}")
    print(f"Failed/skipped updates: {failed_updates}")
    if failed_table_names:
        print("Failed tables:")
        for table_name in failed_table_names:
            print(f" - {table_name}")
    
    # Close connection
    updater.close()
    
    return successful_updates, failed_updates


def main():
    """Run the standalone CLI workflow for `topo_id` synchronization.

    Configures schema names and skipped tables, executes bulk updates, and
    prints a summary of successes and failures.
    """
    
    # Configuration
    current_schema = "release64"
    previous_schema = "release62"
    
    # these tables either come from same source or only loaded into one schema (or both).
    skip_tables = ['contours']
    
    # Create updater instance
    updater = TopoIdUpdater(DB_PARAMS)
    
    print("=== TOPO ID UPDATE UTILITY ===")
    print(f"Current schema: {current_schema}")
    print(f"Previous schema: {previous_schema}")
    print(f"Skip tables: {skip_tables}")
    print()
    

    # Perform the actual updates using extracted logic
    successful, failed = update_all_tables(current_schema, previous_schema, skip_tables)
    
    if successful > 0:
        print(f"\n✓ Successfully updated topo_id values in {successful} tables")
    if failed > 0:
        print(f"\n✗ Failed to update {failed} tables")
    
    updater.close()


if __name__ == "__main__":
    main()