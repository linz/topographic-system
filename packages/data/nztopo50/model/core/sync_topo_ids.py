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
        self.db_params = db_params
        self.conn = None
        self.connect()

    def connect(self):
        """Establish database connection."""
        if self.conn is None or self.conn.closed:
            self.conn = psycopg.connect(**self.db_params)

    def close(self):
        """Close database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()

    def list_schema_tables(self, schema_name):
        """Get list of tables in a schema."""
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
        """Check if a column exists in a table."""
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
        """Check if a table exists in a schema."""
        self.connect()
        query = f"""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = '{table}'
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchone() is not None

    def update_topoid_from_previous_release(self, schema, table, previous_schema):
        """Update topo_id values from previous release based on t50_fid matching."""
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
        """Get statistics about potential topo_id updates across all tables."""
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
    """
    Update topo_id from previous release if requested.
    
    This is the extracted core logic from the original selection.
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
    """Main function for updating topo_id values from previous release."""
    
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