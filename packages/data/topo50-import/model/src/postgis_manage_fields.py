import os
import psycopg
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Database connection parameters
DB_PARAMS = {
    'dbname': 'topo',
    'user': 'postgres',
    'password': 'landinformation',
    'host': 'localhost',
    'port': 5432
}

class ModifyTable:
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

    def column_exists(self, schema, table, column_name, use_like=False):
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
        
    def column_list(self, schema, table, column_name):
        self.connect()
        column_sql = f"like '%{column_name}%'"

        query = f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{table}' AND column_name {column_sql}
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            columns = [row[0] for row in cur.fetchall()]
            return columns

    def add_column(self, table_name, column_name, data_type):
        self.connect()
        with self.conn.cursor() as cur:
            # Check if data_type contains a DEFAULT value
            if "DEFAULT" in data_type:
                query = f'ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS "{column_name}" {data_type};'
            else:
                query = f'ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS "{column_name}" {data_type} DEFAULT NULL;'
            cur.execute(query)
            self.conn.commit()
            print(f"Added column '{column_name}' to table '{table_name}' with data type '{data_type}'")

    def update_column_with_default(self, schema, table, column_name, default_value, where_clause=None):
        self.connect()
        with self.conn.cursor() as cur:
            if where_clause is None:
                update_query = f'UPDATE {schema}.{table} SET {column_name} = {default_value};'
            else:
                update_query = f'UPDATE {schema}.{table} SET {column_name} = {default_value} WHERE {where_clause};'
            cur.execute(update_query)
            self.conn.commit()
            print(f"Updated '{column_name}' in '{schema}.{table}' with default value '{default_value}'")

    def update_default_value(self, schema, table, column_name, default_value):
        self.connect()
        with self.conn.cursor() as cur:
            update_query = f'ALTER TABLE {schema}.{table} ALTER COLUMN {column_name} SET DEFAULT {default_value};'
            cur.execute(update_query)
            self.conn.commit()
            print(f"Updated default value for '{column_name}' in '{schema}.{table}' to '{default_value}'")


    def update_primary_key(self, schema, table, new_primary_key):
        self.connect()
        with self.conn.cursor() as cur:
            # Drop the existing primary key constraint if it exists
            drop_query = f"""ALTER TABLE {schema}.{table} DROP CONSTRAINT IF EXISTS {table}_pkey;"""
            cur.execute(drop_query)
  
            create_sequence = f"CREATE SEQUENCE IF NOT EXISTS {schema}.{table}_{new_primary_key}_seq;"
            cur.execute(create_sequence)

            set_default_query = f"""
                ALTER TABLE {schema}.{table} ALTER COLUMN {new_primary_key} SET DEFAULT nextval('{schema}.{table}_{new_primary_key}_seq');
            """
            cur.execute(set_default_query)

            add_query = f"""ALTER TABLE {schema}.{table} ADD PRIMARY KEY ({new_primary_key})"""
            cur.execute(add_query)
            self.conn.commit()

            print(f"Updated primary key for table '{schema}.{table}' to '{new_primary_key}'")

    def update_primary_key_guid(self, schema, table, new_primary_key):
        self.connect()
        with self.conn.cursor() as cur:
            # Drop the existing primary key constraint if it exists
            drop_query = f"""ALTER TABLE {schema}.{table} DROP CONSTRAINT IF EXISTS {table}_pkey;"""
            cur.execute(drop_query)
  
            # Set the default value for the new primary key column to gen_random_uuid()
            set_default_query = f"""
                ALTER TABLE {schema}.{table} ALTER COLUMN {new_primary_key} SET DEFAULT gen_random_uuid();
            """
            cur.execute(set_default_query)

            # Add the new primary key constraint
            add_query = f"""ALTER TABLE {schema}.{table} ADD PRIMARY KEY ({new_primary_key})"""
            cur.execute(add_query)
            self.conn.commit()

            print(f"Updated primary key for table '{schema}.{table}' to '{new_primary_key}'")


    #may be needed to sqlite - to confirm
    def update_primary_key_seq(self, schema, table, new_primary_key):
        self.connect()
        with self.conn.cursor() as cur:
            # Drop the existing primary key constraint if it exists
            drop_query = f"""ALTER TABLE {schema}.{table} DROP CONSTRAINT IF EXISTS {table}_pkey;"""
            cur.execute(drop_query)
            

            sequence_name = f"{table}_auto_pk_seq"
            create_sequence_sql = f"""
                CREATE SEQUENCE IF NOT EXISTS {schema}.{sequence_name}
                START WITH 1
                INCREMENT BY 1
                MINVALUE 1
                MAXVALUE 9223372036854775807
                CACHE 1;
            """
            
            cur.execute(create_sequence_sql)
#SERIAL 
            # Add the new primary key constraint
            add_query = f"""
                ALTER TABLE {schema}.{table} ADD PRIMARY KEY ({new_primary_key}) 
            """
            cur.execute(add_query)

            # Set default value for the new primary key column
            set_default_query = f"""
                ALTER TABLE {schema}.{table}
                ALTER COLUMN {new_primary_key} SET DEFAULT nextval('{schema}.{sequence_name}'::regclass);
            """
            cur.execute(set_default_query)
            self.conn.commit()
            print(f"Updated primary key for table '{schema}.{table}' to '{new_primary_key}'")

    def add_metadata_columns(self, mode="add", schema_name="toposource", full_field_set=True):
        self.connect()
        schema_tables = self.list_schema_tables(schema_name)

        if full_field_set:
            fieldList = [ 
                ["source", "VARCHAR(75) DEFAULT 'aerial imagery'", "'database import'"], 
                ["source_date", "DATE DEFAULT CURRENT_DATE", "DEFAULT"], 
                ["capture_method", "VARCHAR(25) DEFAULT 'manual'", "DEFAULT"], 
                ["change_type", "VARCHAR(25) DEFAULT 'new'", "DEFAULT"],
                ["update_date", "DATE DEFAULT CURRENT_DATE", "DEFAULT"], 
                ["topo_id", "uuid DEFAULT gen_random_uuid()", "DEFAULT"], 
                ["create_date", "DATE DEFAULT CURRENT_DATE", "DEFAULT"],
                ["version", "INTEGER DEFAULT 1", "DEFAULT"],
            ]
        else:
            fieldList = [ 
                ["source", "VARCHAR(75) DEFAULT 'aerial imagery'", "'database import'"], 
                ["source_date", "DATE DEFAULT CURRENT_DATE", "DEFAULT"], 
            ]
#uuidv7
#["comment", "VARCHAR(255)", "DEFAULT"],

        for schema, tables in schema_tables.items():
            for table in tables:
                for column_name, data_type, default_value in fieldList:
                    try:
                        if mode == "add":
                            if not self.column_exists(schema, table, column_name):
                                self.add_column(f'"{schema}"."{table}"', column_name, data_type)
                            if default_value != "DEFAULT":
                                self.update_column_with_default(schema, table, column_name, default_value)
                        elif mode == "alter":
                            self.alter_column(schema, table, column_name, data_type)
                    except Exception as e:
                        print(f"Error '{column_name}' in table '{schema}.{table}': {e}")


    def alter_column(self, schema, table_name, column_name, data_type):

        default_value = data_type.split("DEFAULT", 1)[1].strip()
        query = f'ALTER TABLE {schema}.{table_name} ALTER COLUMN "{column_name}" SET DEFAULT {default_value};'
        with self.conn.cursor() as cur:
            cur.execute(query)
            self.conn.commit()

    def populate_defined_null_values(self, schema_name):
        self.connect()
        schema_tables = self.list_schema_tables(schema_name)

        # Define a dictionary with key as "schema.table" and value as a list of (column_name, update_value) tuples
        update_dict = {
            f"{schema_name}.runway": [("surface", "'grass'","")],
            f"{schema_name}.vegetation": [("species", "'coniferous'","AND feature_type = 'exotic'")],
            f"{schema_name}.road_line": [("level", "0","")],
            f"{schema_name}.railway_line": [("vehicle_type", "'train'","")],
            # Add more entries as needed
        }

        for schema, tables in schema_tables.items():
            for table in tables:
                if f"{schema}.{table}" in update_dict:
                    for column_name, update_value, and_statement in update_dict[f"{schema}.{table}"]:
                        try:
                            if self.column_exists(schema, table, column_name):
                                update_query = f'UPDATE {schema}.{table} SET {column_name} = {update_value} WHERE {column_name} IS NULL {and_statement};'
                                with self.conn.cursor() as cur:
                                    cur.execute(update_query)
                                    self.conn.commit()
                                    print(f"Updated '{column_name}' in '{schema}.{table}' with value '{update_value}'")
                        except Exception as e:
                            print(f"Error updating '{column_name}' in table '{schema}.{table}': {e}")
                
    def set_default_values(self, schema_name="toposource"):
            self.connect()

            # Define a dictionary with key as "schema.table" and value as a list of (column_name, update_value) tuples
            update_dict = {
                f"{schema_name}.runway": [("status", "'active'"),("surface", "'sealed'")],
                f"{schema_name}.airport": [("feature_type", "'airport'")],
                f"{schema_name}.bridge_line": [("feature_type", "'bridge'")],
                f"{schema_name}.building": [("feature_type", "'building'")],
                f"{schema_name}.building_point": [("feature_type", "'building'")],
                f"{schema_name}.descriptive_text": [("feature_type", "'descriptive_text'")],
                f"{schema_name}.railway_line": [("feature_type", "'railway'")],
                f"{schema_name}.railway_station": [("feature_type", "'railway_station'")],
                f"{schema_name}.residential_area": [("feature_type", "'residential_area'")],
                f"{schema_name}.road_line": [("feature_type", "'road'")],
                f"{schema_name}.track_line": [("feature_type", "'track'")],
                f"{schema_name}.tree_locations": [("feature_type", "'tree'")],
                f"{schema_name}.trig_point": [("feature_type", "'trig'")],
                f"{schema_name}.tunnel_line": [("feature_type", "'tunnel'")],
            }

                #f"{schema_name}.river": [("feature_type", "'river'")],
                #f"{schema_name}.river_line": [("feature_type", "'river'")],

            with self.conn.cursor() as cur:
                for table_name, columns in update_dict.items():
                    schema, table = table_name.split('.')
                    for column_name, default_value in columns:
                        update_query = f'ALTER TABLE {schema}.{table} ALTER COLUMN {column_name} SET DEFAULT {default_value};'
                        cur.execute(update_query)

    def recreate_table_srid(self, schema_name="toposource", primary_key_type='int'):
        self.connect()
        schema_tables = self.list_schema_tables(schema_name)
        

        for schema, tables in schema_tables.items():
            for table in tables:
                if table == 'collections': 
                    continue
                fields = self.get_ordered_columns(schema, table, primary_key_type)
                if self.column_exists(schema, table, "geom"):
                    geom_field = "geom"
                else:
                    geom_field = "ST_Transform(geometry, 4167) AS geom"
                if not self.table_exists(schema, f"{table}_4167"):
                    create_query = f"""
                        CREATE TABLE "{schema}"."{table}_4167" AS
                        SELECT {', '.join([f'"{field}"' for field in fields])},
                               {geom_field} 
                        FROM "{schema}"."{table}";
                    """
                    
                    with self.conn.cursor() as cur:
                        cur.execute(create_query)
                        self.conn.commit()
                        print(f"Created table '{schema}.{table}_4167' with SRID 4167")

                # Drop the original table and rename the new table
                with self.conn.cursor() as cur:
                    drop_query = f'DROP TABLE "{schema}"."{table}" CASCADE;'
                    rename_query = f'ALTER TABLE "{schema}"."{table}_4167" RENAME TO "{table}";'
                    cur.execute(drop_query)
                    cur.execute(rename_query)
                    self.conn.commit()
                    print(f"Dropped original table '{schema}.{table}' and renamed '{schema}.{table}_4167' to '{schema}.{table}'")

                #Add indexes
                index_sql = f"CREATE INDEX IF NOT EXISTS idx_{table}_geom ON {schema}.{table} USING GIST (geom);"
                with self.conn.cursor() as cur:
                    try:
                        cur.execute(index_sql)
                        self.conn.commit()
                    except Exception as e:
                        print(f"Error creating index for '{table}': {e}")
                        continue
                print(f"Index for '{table}' created successfully.")


                index_fields = ['use', 'type', 'name']
                for field_name in index_fields:
                    columns = tableModifer.column_list(schema, table, field_name)
                    if columns:
                        for field in columns:
                            if field == 'change_type': 
                                continue
                            sql = f"CREATE INDEX IF NOT EXISTS idx_{table}_{field_name} ON {schema}.{table}({field});"
                            with self.conn.cursor() as cur:
                                try:
                                    cur.execute(sql)
                                except Exception as e:
                                    print(f"Error creating index for '{table}': {e}")
                                    continue


                #index_sql = f"CREATE INDEX IF NOT EXISTS idx_{table}_class ON {schema}.{table}(feature_type);"
                #with self.conn.cursor() as cur:
                #    try:
                #        cur.execute(index_sql)
                #        self.conn.commit()
                #    except Exception as e:
                #        print(f"Error creating index for '{table}': {e}")
                #        continue
                #print(f"Index for '{table}' created successfully.")

    
    def add_name_columns(self):
        self.connect()
        table_list = [
            "physical_infrastructure_point",
            "physical_infrastructure_line",
            "structure",
            "vegetation",
            "landcover",
            "landcover_line",
            "ferry_crossing",
        ]

        for table in table_list:
            schema = self.table_schema(table)[0]
            if not tableModifer.column_exists(schema, table, "name"):
                tableModifer.add_column(f'"{schema}"."{table}"', "name", "VARCHAR(50)")

    def add_collectionid_columns(self):
        self.connect()
        table_list = [
            "road_line",
            "ferry_crossing",
            "water",
            "water_line",
            "water_point",
            "coastline",
            "island",
            "railway_station",
            "railway_line",
        ]

        for table in table_list:
            schema = self.table_schema(table)[0]
            if not tableModifer.column_exists(schema, table, "collection_id"):
                tableModifer.add_column(f'"{schema}"."{table}"', "collection_id", "uuid")
            #if not tableModifer.column_exists(schema, table, "collection_name"):
            #    tableModifer.add_column(f'"{schema}"."{table}"', "collection_name", "VARCHAR(100)")

    def create_collections_table(self, schema_name="toposource"):
        self.connect()
        with self.conn.cursor() as cur:
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS "{schema_name}"."collections" (
                    collection_id uuid PRIMARY KEY,
                    topo_id uuid,
                    source_table VARCHAR(100),
                    collection_name VARCHAR(255)
                );
            """
            cur.execute(create_table_sql)
            self.conn.commit()
            print(f'Created table "{schema_name}.collections" with columns collection_id (uuid, primary key) and topo_id (uuid)')
    
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
            if self.column_exists(schema, table, base_column_name) and self.column_exists(schema, table, drop_column_name):
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

    def drop_column(self, schema, table, drop_column_name):
        self.connect()
        with self.conn.cursor() as cur:
            if self.column_exists(schema, table, drop_column_name):
                drop_query = f'ALTER TABLE {schema}.{table} DROP COLUMN {drop_column_name};'
                cur.execute(drop_query)

                print(f"Dropped column '{drop_column_name}' from table '{schema}.{table}'")
            else:
                print(f"Column '{drop_column_name}' does not exist in table '{schema}.{table}'")



    def update_value_by_column(self, schema, table, column_name, from_column_name):
        self.connect()
        with self.conn.cursor() as cur:
            if self.column_exists(schema, table, column_name):
                if from_column_name.lower() == 'null':
                    update_query = f'UPDATE {schema}.{table} SET {column_name} = NULL;'
                else:
                    update_query = f'UPDATE {schema}.{table} SET {column_name} = {from_column_name};'
                cur.execute(update_query)
                self.conn.commit()
                print(f"Set value of column '{column_name}' to '{from_column_name}' in table '{schema}.{table}'")
            else:
                print(f"Column '{column_name}' does not exist in table '{schema}.{table}'")

    def update_topoid_from_previous_release(self, schema, table, previous_schema):
        self.connect()
        with self.conn.cursor() as cur:
            if self.column_exists(schema, table, "t50_fid"):
                update_query = f'''
                    UPDATE {schema}.{table} AS new
                    SET topo_id = old.topo_id
                    FROM {previous_schema}.{table} AS old
                    WHERE new.t50_fid = old.t50_fid;
                '''
                cur.execute(update_query)
                self.conn.commit()
                print(f"Updated 'topo_id' in '{schema}.{table}' from '{previous_schema}.{table}'")
            else:
                print(f"'t50_fid' column does not exist in one of the tables '{schema}.{table}' or '{previous_schema}.{table}'")


    def all_ordered_columns(self, primary_key_type='int'):
        ordered_list = [
            "id",
            "t50_fid",
            "topo_id",
            "feature_type",
            "bridge_use",
            "bridge_use2",
            "building_use",
            "elevation_use",
            "relief_use",
            "infrastructure_use",
            "landcover_use",
            "landuse_use",
            "railway_use",
            "runway_use",
            "shaft_use",
            "structure_use",
            "tunnel_use",
            "tunnel_use2",
            "track_use",
            "construction_type",
            "water_use",
            "lid_type",
            "support_type",
            "tank_type",
            "track_type",
            "landcover_type",
            "landuse_type",
            "tunnel_type",
            "vehicle_type",
            "structure_type",
            "trig_type",
            "sub_type",
            "coastline_type",
            "place_type",
            "hierarchy",
            "level",
            "species",
            "species_cultivated",
            "status",
            "name",
            "group_name",
            "nzgb_id",
            "code",
            "info_display",
            "location",
            "highway_number",
            "sheet_code",
            "sheet_name",
            "elevation",
            "composition",
            "desc_code",
            "description",
            "display",
            "definition",
            "designated",
            "designation",
            "formation",
            "edition",
            "height",
            "orientation",
            "lane_count",
            "material",
            "materials",
            "material_conveyed",
            "perennial",
            "restrictions",
            "revised",
            "size",
            "stored_item",
            "substance",
            "surface",
            "temperature",
            "temperature_indicator",
            "visibility",
            "way_count",
            "road_access",
            "width",
            "name_id",
            "wreck_of",
            "shape_area",
            "rna_sufi",
            "route",
            "route2",
            "route3",
            "collection_id",
            "collection_name",
            "theme",
            "source",
            "source_date",
            "capture_method",
            "change_type",
            "update_date",
            "create_date",
            "version"
        ]

        if primary_key_type == 'uuid':
            ordered_list.remove("id")
            ordered_list.remove("topo_id")
            ordered_list.insert(0, "topo_id")
        return ordered_list
    
    def get_non_compare_columns(self):
        """
        Returns a list of columns that should not be compared when checking for differences between tables.
        """
        non_compare_columns = [
            "topo_id",
            "t50_fid",
            "feature_type",
            "theme",
            "source",
            "source_date",
            "capture_method",
            "change_type",
            "update_date",
            "create_date",
            "version"
        ]
        return non_compare_columns
    
    def get_ordered_columns(self, schema, table, primary_key_type='int'):
        """
        Returns a list of columns in the specified schema and table, ordered according to the predefined order.
        """
        self.connect()
        ordered_columns = self.all_ordered_columns(primary_key_type)
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table))
            existing_columns = [row[0] for row in cur.fetchall()]
        
        # Filter and order the existing columns according to the predefined order
        return [col for col in ordered_columns if col in existing_columns]


    def get_compare_columns(self, schema, table):
        """
        Returns a list of columns that should be compared when checking for differences between tables.
        """
        self.connect()
        non_compare_columns = self.get_non_compare_columns()
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table))
            existing_columns = [row[0] for row in cur.fetchall()]
        
        # Filter and order the existing columns according to the predefined order
        compare_columns = [col for col in self.all_ordered_columns() if col in existing_columns and col not in non_compare_columns]
        return compare_columns
    
    def compare_table_data(self, schema, table, previous_schema, release_date=None):
        self.connect()
        compare_columns = self.get_compare_columns(schema, table)
        if not compare_columns:
            print(f"No comparable columns found for {schema}.{table}")
            return None

        #INSERTS
        query = f"""
            SELECT b.topo_id::text, '{table}' as table_name, b.feature_type, 'added' AS change_type
            FROM {schema}.{table} AS b
            LEFT JOIN {previous_schema}.{table} AS a ON a.topo_id = b.topo_id
            WHERE a.topo_id IS NULL; 
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            insert_df = pd.DataFrame(rows, columns=columns)
        
        #DELETES
        query = f"""
            SELECT a.topo_id::text, '{table}' as table_name, a.feature_type, 'removed' AS change_type
            FROM {previous_schema}.{table} AS a
            LEFT JOIN {schema}.{table} AS b ON a.topo_id = b.topo_id
            WHERE b.topo_id IS NULL; 
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            delete_df = pd.DataFrame(rows, columns=columns)
            
        #UPDATES
        # Build the IS DISTINCT FROM conditions dynamically for all compare_columns
        distinct_conditions = " OR ".join(
            [f"a.{col} IS DISTINCT FROM b.{col}" for col in compare_columns]
        )
        query = f"""
            SELECT a.topo_id::text, '{table}' as table_name, a.feature_type, 'updated' AS change_type
            FROM {previous_schema}.{table} AS a
            JOIN {schema}.{table} AS b ON a.topo_id = b.topo_id
            WHERE {distinct_conditions};
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            update_df = pd.DataFrame(rows, columns=columns)

        # Split release_date (format yyyy-mm-dd) into year, month, day columns
        if release_date:
            year, month, day = release_date.split('-')
            insert_df['year'] = year
            insert_df['month'] = month
            insert_df['day'] = day
            delete_df['year'] = year
            delete_df['month'] = month
            delete_df['day'] = day
            update_df['year'] = year
            update_df['month'] = month
            update_df['day'] = day
        return insert_df, delete_df, update_df

if __name__ == "__main__":
    tableModifer = ModifyTable(DB_PARAMS)
    option = "all"
    #option = "recreate_table_srid"
    #schema_name = "toposource"
    schema_name = "release62"
    release_date = "2025-02-05"
    
    #schema_name = "release63"
    #release_date = "2025-05-14"

    add_full_metadata_fields = True
    #primary_key_type = 'int' #uuid
    primary_key_type = 'uuid'
    update_topoid_from_previous_release = False
    previous_schema = "release62"
    use_hive_partitioning = True
    change_logs_path = r"c:\data\topo-data-vector\changelogs"
    if update_topoid_from_previous_release:
        if not os.path.exists(change_logs_path):
            os.makedirs(change_logs_path)

    if option == "all" or option == "metadata":
        tableModifer.add_metadata_columns(mode='add', schema_name=schema_name,full_field_set=add_full_metadata_fields)

#TEST -= DOUBLE CHECK THIS WORKING AS EXPECTED
    if option == "all" or option == "columns":
        update_dict = {
            (schema_name, "structure_point"): [("structure_use", "shaft_use"),
                          ("structure_type", "tank_type"),
                          ("material", "materials")],
            (schema_name, "structure_line"): [("status", "dam_status")],
            (schema_name, "structure"): [("species", "species_cultivated"), ("lid_type", "reservoir_lid_type"),
                          ("structure_type", "tank_type")],
            (schema_name, "road_line"): [("highway_number", "highway_numb")],
            (schema_name, "water"): [("water_use", "pond_use"), ("height", "elevation")]
        }

        for (schema, table), columns in update_dict.items():
            for base_col, drop_col in columns:
                tableModifer.set_base_column_and_drop_column(schema, table, base_col, drop_col) 
        tableModifer.drop_column(schema, "tree_locations", 'name')   

        #specific updates
        tableModifer.update_column_with_default(schema_name, "tunnel_line", "tunnel_use2", "'livestock'", "tunnel_use2 ='ivestock'")
        tableModifer.update_column_with_default(schema_name, "tunnel_line", "tunnel_use", "'vehicle'", "tunnel_use2 ='vehicle'")
        tableModifer.update_column_with_default(schema_name, "tunnel_line", "tunnel_use2", "'livestock'", "tunnel_use2 ='vehicle'")

        tableModifer.update_column_with_default(schema_name, "trig_point", "trig_type", "'beacon'")

        tableModifer.update_column_with_default(schema_name, "road_line", "way_count", "'one way'", "way_count ='1'")
        if tableModifer.column_exists(schema_name, "road_line", "road_access"):
            tableModifer.update_column_with_default(schema_name, "road_line", "road_access", "'mp'", "road_access ='m'")
      
        tableModifer.update_column_with_default(schema_name, "physical_Infrastructure_line", "support_type", "'pole'", "feature_type ='telephone'")
      

    if option == "all" or option == "name":
        tableModifer.add_name_columns()
        tableModifer.add_collectionid_columns()

    if option == "all" or option == "additions":
        tableModifer.add_column(f"{schema_name}.trig_point", "code", "VARCHAR(20)")
        tableModifer.update_value_by_column(schema_name, "trig_point", "code", "name")
        tableModifer.update_value_by_column(schema_name, "trig_point", "name", "null")

        tableModifer.add_column(f"{schema_name}.vegetation", "sub_type", "VARCHAR(50)")
        tableModifer.update_value_by_column(schema_name, "vegetation", "sub_type", "species")
        tableModifer.update_value_by_column(schema_name, "vegetation", "species", "null")

        tableModifer.add_column(f"{schema_name}.landcover", "sub_type", "VARCHAR(50)")

        tableModifer.add_column(f"{schema_name}.road_line", "level", "INTEGER")
        tableModifer.add_column(f"{schema_name}.road_line", "hierarchy", "VARCHAR(50)")

        tableModifer.add_column(f"{schema_name}.descriptive_text", "nzgb_id", "BIGINT")
        tableModifer.add_column(f"{schema_name}.railway_line", "nzgb_id", "BIGINT")
        tableModifer.add_column(f"{schema_name}.railway_line", "route", "VARCHAR(30)")
        tableModifer.add_column(f"{schema_name}.railway_line", "route2", "VARCHAR(30)")
        tableModifer.add_column(f"{schema_name}.railway_line", "route3", "VARCHAR(30)")

        tableModifer.add_column(f"{schema_name}.coastline", "coastline_type", "VARCHAR(50)")
        
        tableModifer.add_column(f"{schema_name}.road_line", "hierarchy", "VARCHAR(25)")
        #tableModifer.add_column(f"{schema_name}.river_line", "hierarchy", "VARCHAR(25)")
        #tableModifer.add_column(f"{schema_name}.river", "hierarchy", "VARCHAR(25)") 
        #tableModifer.add_column(f"{schema_name}.river", "railway_line", "VARCHAR(25)")
        tableModifer.add_column(f"{schema_name}.water_line", "hierarchy", "VARCHAR(25)")
        tableModifer.add_column(f"{schema_name}.water", "hierarchy", "VARCHAR(25)") 
        tableModifer.add_column(f"{schema_name}.water", "railway_line", "VARCHAR(25)")

        tableModifer.rename_columns(schema_name, "contour", "nat_form", "formation")
        tableModifer.rename_columns(schema_name, "contour", "designated", "designation")

        tableModifer.rename_columns(schema_name, "landuse", "track_type", "landuse_type")
        tableModifer.update_value_by_column(schema_name, "landuse", "landuse_type", "visibility")
        tableModifer.drop_column(schema_name, "landuse", "visibility")
        tableModifer.rename_columns(schema_name, "landuse_line", "track_type", "landuse_type")
        tableModifer.rename_columns(schema_name, "place_point", "visibility", "place_type")

        #offshore (1) or inland island (0) - added manually using sea_coastline poly shapefile create from coastline and box
        #tableModifer.add_column(f"{schema_name}.island", "location", "INTEGER")
        
 

    if option == "all" or option == "null_updates":
        tableModifer.populate_defined_null_values(schema_name)

    if option == "all" or option == "defaults":
        tableModifer.set_default_values(schema_name)

    if option == "all" or option == "rename":
        rename_dict = {
            f"{schema_name}.structure_line": [("materials", "material"), ("mtlconveyd", "material_conveyed")],
            f"{schema_name}.structure_point": [("store_item", "stored_item")],
            f"{schema_name}.structure": [("store_item", "stored_item")],
        }

        for table_full, columns in rename_dict.items():
            schema, table = table_full.split(".")
            for old_column_name, new_column_name in columns:
                tableModifer.rename_columns(schema, table, old_column_name, new_column_name)

    if option == "all" or option == "recreate_table_srid":
        tableModifer.recreate_table_srid(schema_name, primary_key_type)
        tableModifer.add_metadata_columns('alter', schema_name, add_full_metadata_fields)

    if option == "all" or option == "primary_key":
        schema_tables = tableModifer.list_schema_tables(schema_name)
        for schema, tables in schema_tables.items():
            #all tables processed from any esri fields
            tableModifer.drop_column(schema, table, 'ESRI_OID')
            if primary_key_type == 'int':
                for table in tables:
                    tableModifer.update_primary_key(schema, table, "id")
            else:
                for table in tables:
                    tableModifer.update_primary_key_guid(schema, table, "topo_id")

    if option == "all" or option == "collections":
        tableModifer.create_collections_table(schema_name)

    if update_topoid_from_previous_release and option == "all":
        schema_tables = tableModifer.list_schema_tables(schema_name)
        for schema, tables in schema_tables.items():
            for table in tables:
                tableModifer.update_topoid_from_previous_release(schema, table, previous_schema)

    if update_topoid_from_previous_release and (option == "all" or option == "compare"):
        schema_tables = tableModifer.list_schema_tables(schema_name)
        df_order = ['added', 'removed', 'updated']
        i = 0
        for schema, tables in schema_tables.items():
            for table in tables:
                if table == 'collections': continue
                dfs = tableModifer.compare_table_data(schema, table, previous_schema, release_date)
                i+=1
                if dfs is None:
                    print(f"No comparable columns for {schema}.{table}, skipping comparison.")
                    continue
                else:
                    if i == 1:
                        insert_df, delete_df, update_df = dfs
                    else:
                        insert_df_new, delete_df_new, update_df_new = dfs
                        insert_df = pd.concat([insert_df, insert_df_new], ignore_index=True)
                        delete_df = pd.concat([delete_df, delete_df_new], ignore_index=True)
                        update_df = pd.concat([update_df, update_df_new], ignore_index=True)
                    
                    all_dfs = [insert_df, delete_df, update_df]
        i = 0
        for df in all_dfs:
            if df.empty:
                print(f"No {df_order[i]} changes detected for {schema}.{table}")
            else:
                if not use_hive_partitioning:
                    output_file = os.path.join(change_logs_path, schema, f"{schema}_{table}_{df_order[i]}_changelog.parquet")
                    os.makedirs(os.path.dirname(output_file), exist_ok=True)
                    df.to_parquet(output_file, index=False)
                    print(f"Exported {df_order[i]} changes for {schema}.{table} to {output_file}")
                else:
                    partition_path = os.path.join(change_logs_path, schema)

                    os.makedirs(partition_path, exist_ok=True)
                    partition_path = partition_path.replace("\\", "/")
                    output_file = os.path.join(partition_path, f"{schema}_{table}_{df_order[i]}_changelog.parquet")

                    pq.write_to_dataset(pa.Table.from_pandas(df), root_path=partition_path, partition_cols=['year', 'month', 'day', 'change_type'])
                    print(f"Exported {df_order[i]} changes for {schema}.{table} to {output_file}")
            i += 1
