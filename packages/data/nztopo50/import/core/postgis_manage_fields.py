import psycopg

# Database connection parameters
DB_PARAMS = {
    "dbname": "topo",
    "user": "postgres",
    "password": "landinformation",
    "host": "localhost",
    "port": 5432,
}


class ModifyTable:
    """DDL and DML utilities for managing PostGIS table schema and field values.

    Provides a collection of methods for adding, renaming, and dropping columns,
    managing primary keys, updating field values, and restructuring tables for
    Topo50 data releases.
    """

    def __init__(self, db_params):
        """Initialize the table modifier and open a database connection.

        Args:
            db_params: Mapping of connection arguments accepted by
                `psycopg.connect`.
        """
        self.db_params = db_params
        self.conn = None
        self.connect()

    def connect(self):
        """Establish a database connection if none is currently open."""
        if self.conn is None or self.conn.closed:
            self.conn = psycopg.connect(**self.db_params)

    def close(self):
        """Close the active database connection if open."""
        if self.conn and not self.conn.closed:
            self.conn.close()

    def list_schema_tables(self, schema_name):
        """List all tables contained in a schema.

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

    def table_schema(self, table):
        """Return the schema that owns a table.

        Args:
            table: Table name to look up.

        Returns:
            tuple | None: Row containing `table_schema`, or None if not found.
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
        """Check whether a table exists in a schema.

        Args:
            schema: Schema name.
            table: Table name.

        Returns:
            bool: True when the table exists.
        """
        self.connect()
        query = """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (schema, table))
            return cur.fetchone() is not None

    def column_exists(self, schema, table, column_name, use_like=False):
        """Check whether a column exists in a table.

        Args:
            schema: Schema name.
            table: Table name.
            column_name: Exact column name, or partial name when `use_like` is True.
            use_like: If True, use SQL `LIKE` matching on `column_name`.

        Returns:
            bool: True when a matching column is found.
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

    def column_list(self, schema, table, column_name):
        """Return columns whose names contain a given substring.

        Args:
            schema: Schema name.
            table: Table name.
            column_name: Substring to match against column names.

        Returns:
            list[str]: Column names matching the pattern.
        """
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
        """Add a column to a table if it does not already exist.

        Args:
            table_name: Fully qualified table name (e.g. ``"schema"."table"``
                or ``schema.table``).
            column_name: Column to add.
            data_type: SQL data type string. If it already includes a
                ``DEFAULT`` clause it is used verbatim; otherwise
                ``DEFAULT NULL`` is appended.
        """
        self.connect()
        with self.conn.cursor() as cur:
            # Check if data_type contains a DEFAULT value
            if "DEFAULT" in data_type:
                query = f'ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS "{column_name}" {data_type};'
            else:
                query = f'ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS "{column_name}" {data_type} DEFAULT NULL;'
            cur.execute(query)
            self.conn.commit()
            print(
                f"Added column '{column_name}' to table '{table_name}' with data type '{data_type}'"
            )

    def get_srid(self, schema, table, geometry_field_name="geometry"):
        """Return the SRID registered for a geometry column.

        Args:
            schema: Schema name.
            table: Table name.
            geometry_field_name: Geometry column name (default ``geometry``).

        Returns:
            int | None: SRID value, or None when the column is not found.
        """
        self.connect()
        query = f"""
            SELECT Find_SRID('{schema}', '{table}', '{geometry_field_name}');
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()
            return result[0] if result else None

    def get_geometry_type(self, schema, table, geometry_field_name="geometry"):
        """Return the geometry type string for a column (e.g. ``POLYGON``).

        Args:
            schema: Schema name.
            table: Table name.
            geometry_field_name: Geometry column name (default ``geometry``).

        Returns:
            str | None: Geometry type, or None when the table is empty or the
            column does not exist.
        """
        self.connect()
        query = f"""
            SELECT GeometryType("{geometry_field_name}") FROM "{schema}"."{table}" LIMIT 1;
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()
            return result[0] if result else None

    def carto_text_geom_update(self, schema, table):
        """Snap all geometry coordinates to a 1-metre grid using ST_SnapToGrid.

        Args:
            schema: Schema name.
            table: Table name whose ``geometry`` column will be updated.
        """
        self.connect()
        with self.conn.cursor() as cur:
            update_query = (
                f"UPDATE {schema}.{table} SET geometry = ST_SnapToGrid(geometry, 1.0);"
            )
            cur.execute(update_query)
            self.conn.commit()
            print(f"Updated geometry field in '{schema}.{table}' with ST_SnapToGrid")

    def update_column_with_default(
        self, schema, table, column_name, default_value, where_clause=None
    ):
        """Set a column to a literal value for matching rows.

        Args:
            schema: Schema name.
            table: Table name.
            column_name: Column to update.
            default_value: Literal SQL value to assign (e.g. ``"'active'"`` or
                ``0``).
            where_clause: Optional SQL ``WHERE`` predicate. When omitted all
                rows are updated.
        """
        self.connect()
        with self.conn.cursor() as cur:
            if where_clause is None:
                update_query = (
                    f"UPDATE {schema}.{table} SET {column_name} = {default_value};"
                )
            else:
                update_query = f"UPDATE {schema}.{table} SET {column_name} = {default_value} WHERE {where_clause};"
            cur.execute(update_query)
            self.conn.commit()
            print(
                f"Updated '{column_name}' in '{schema}.{table}' with default value '{default_value}'"
            )

    def update_default_value(self, schema, table, column_name, default_value):
        """Set the DDL default expression for a column.

        Args:
            schema: Schema name.
            table: Table name.
            column_name: Column whose default is to be changed.
            default_value: SQL expression to use as the new default.
        """
        self.connect()
        with self.conn.cursor() as cur:
            update_query = f"ALTER TABLE {schema}.{table} ALTER COLUMN {column_name} SET DEFAULT {default_value};"
            cur.execute(update_query)
            self.conn.commit()
            print(
                f"Updated default value for '{column_name}' in '{schema}.{table}' to '{default_value}'"
            )

    def update_primary_key(self, schema, table, new_primary_key):
        """Replace the primary key with an integer sequence-backed column.

        Drops any existing primary key constraint, creates a supporting
        sequence, wires it as the column default, and adds the constraint.

        Args:
            schema: Schema name.
            table: Table name.
            new_primary_key: Column to promote to primary key.
        """
        self.connect()
        with self.conn.cursor() as cur:
            # Drop the existing primary key constraint if it exists
            drop_query = f"""ALTER TABLE {schema}.{table} DROP CONSTRAINT IF EXISTS {table}_pkey;"""
            cur.execute(drop_query)

            create_sequence = (
                f"CREATE SEQUENCE IF NOT EXISTS {schema}.{table}_{new_primary_key}_seq;"
            )
            cur.execute(create_sequence)

            set_default_query = f"""
                ALTER TABLE {schema}.{table} ALTER COLUMN {new_primary_key} SET DEFAULT nextval('{schema}.{table}_{new_primary_key}_seq');
            """
            cur.execute(set_default_query)

            add_query = (
                f"""ALTER TABLE {schema}.{table} ADD PRIMARY KEY ({new_primary_key})"""
            )
            cur.execute(add_query)
            self.conn.commit()

            print(
                f"Updated primary key for table '{schema}.{table}' to '{new_primary_key}'"
            )

    def update_primary_key_guid(self, schema, table, new_primary_key):
        """Replace the primary key with a UUID column backed by gen_random_uuid().

        Drops any existing primary key constraint, sets the column default to
        ``gen_random_uuid()``, and adds the new constraint.

        Args:
            schema: Schema name.
            table: Table name.
            new_primary_key: Column to promote to primary key.
        """
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
            add_query = (
                f"""ALTER TABLE {schema}.{table} ADD PRIMARY KEY ({new_primary_key})"""
            )
            cur.execute(add_query)
            self.conn.commit()

            print(
                f"Updated primary key for table '{schema}.{table}' to '{new_primary_key}'"
            )

    # may be needed to sqlite - to confirm
    def update_primary_key_seq(self, schema, table, new_primary_key):
        """Replace the primary key using an explicit named sequence.

        Creates a dedicated sequence ``{table}_auto_pk_seq``, adds the primary
        key constraint, and sets the column default to ``nextval`` of that
        sequence.

        Args:
            schema: Schema name.
            table: Table name.
            new_primary_key: Column to promote to primary key.
        """
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
            # SERIAL
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
            print(
                f"Updated primary key for table '{schema}.{table}' to '{new_primary_key}'"
            )

    def add_metadata_columns(
        self,
        mode="add",
        schema_name="toposource",
        full_field_set=True,
        include_source_fields=False,
    ):
        """Add or alter standard metadata columns across all tables in a schema.

        Args:
            mode: ``'add'`` to add missing columns and populate them, or
                ``'alter'`` to update the DDL default only.
            schema_name: Target schema.
            full_field_set: When True, includes ``capture_method``,
                ``change_type``, ``update_date``, ``topo_id``, ``create_date``, and
                ``version``.
            include_source_fields: When True, also adds ``source``,
                ``source_id``, and ``source_date`` columns.
        """
        self.connect()
        schema_tables = self.list_schema_tables(schema_name)

        if full_field_set:
            fieldList = [
                ["capture_method", "VARCHAR(25) DEFAULT 'manual'", "DEFAULT"],
                ["change_type", "VARCHAR(25) DEFAULT 'new'", "DEFAULT"],
                ["update_date", "DATE DEFAULT CURRENT_DATE", "DEFAULT"],
                ["topo_id", "uuid DEFAULT gen_random_uuid()", "DEFAULT"],
                ["create_date", "DATE DEFAULT CURRENT_DATE", "DEFAULT"],
                ["version", "INTEGER DEFAULT 1", "DEFAULT"],
            ]
        if include_source_fields:
            fieldList.extend(
                [
                    [
                        "source",
                        "VARCHAR(75) DEFAULT 'nz aerial imagery'",
                        "'database import'",
                    ],
                    ["source_id", "INTEGER", "DEFAULT"],
                    ["source_date", "DATE DEFAULT CURRENT_DATE", "DEFAULT"],
                ]
            )
        # uuidv7
        # ["comment", "VARCHAR(255)", "DEFAULT"],

        for schema, tables in schema_tables.items():
            for table in tables:
                for column_name, data_type, default_value in fieldList:
                    try:
                        if mode == "add":
                            if not self.column_exists(schema, table, column_name):
                                self.add_column(
                                    f'"{schema}"."{table}"', column_name, data_type
                                )
                            if default_value != "DEFAULT":
                                self.update_column_with_default(
                                    schema, table, column_name, default_value
                                )
                        elif mode == "alter":
                            self.alter_column(schema, table, column_name, data_type)
                    except Exception as e:
                        print(f"Error '{column_name}' in table '{schema}.{table}': {e}")

    def alter_column(self, schema, table_name, column_name, data_type):
        """Update the DDL default of a column by parsing it from a type string.

        Args:
            schema: Schema name.
            table_name: Table name.
            column_name: Column whose default is to be altered.
            data_type: SQL type string containing a ``DEFAULT`` clause from
                which the new default expression is extracted.
        """
        default_value = data_type.split("DEFAULT", 1)[1].strip()
        query = f'ALTER TABLE {schema}.{table_name} ALTER COLUMN "{column_name}" SET DEFAULT {default_value};'
        with self.conn.cursor() as cur:
            cur.execute(query)
            self.conn.commit()

    def populate_defined_null_values(self, schema_name):
        """Fill NULL values with predefined defaults for specific columns.

        A hard-coded mapping controls which tables and columns receive updates.
        Only rows where the column is currently NULL are affected.

        Args:
            schema_name: Schema containing the target tables.
        """
        self.connect()
        schema_tables = self.list_schema_tables(schema_name)

        # Define a dictionary with key as "schema.table" and value as a list of (column_name, update_value) tuples
        update_dict = {
            f"{schema_name}.runway": [("surface", "'grass'", "")],
            f"{schema_name}.vegetation": [
                ("species", "'coniferous'", "AND feature_type = 'exotic'")
            ],
            f"{schema_name}.railway_line": [("vehicle_type", "'train'", "")],
            # Add more entries as needed - should be pre-existing
        }

        for schema, tables in schema_tables.items():
            for table in tables:
                if f"{schema}.{table}" in update_dict:
                    for column_name, update_value, and_statement in update_dict[
                        f"{schema}.{table}"
                    ]:
                        try:
                            if self.column_exists(schema, table, column_name):
                                update_query = f"UPDATE {schema}.{table} SET {column_name} = {update_value} WHERE {column_name} IS NULL {and_statement};"
                                with self.conn.cursor() as cur:
                                    cur.execute(update_query)
                                    self.conn.commit()
                                    print(
                                        f"Updated '{column_name}' in '{schema}.{table}' with value '{update_value}'"
                                    )
                        except Exception as e:
                            print(
                                f"Error updating '{column_name}' in table '{schema}.{table}': {e}"
                            )

    def set_default_values(self, schema_name="toposource"):
        """Apply DDL default expressions for feature_type and status columns.

        Updates ``ALTER COLUMN … SET DEFAULT`` for a curated set of tables so
        that new rows automatically receive the correct attribute values.

        Args:
            schema_name: Schema containing the target tables.
        """
        self.connect()

        # Define a dictionary with key as "schema.table" and value as a list of (column_name, update_value) tuples
        update_dict = {
            f"{schema_name}.runway": [("status", "'active'"), ("surface", "'sealed'")],
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

        # f"{schema_name}.river": [("feature_type", "'river'")],
        # f"{schema_name}.river_line": [("feature_type", "'river'")],

        with self.conn.cursor() as cur:
            for table_name, columns in update_dict.items():
                schema, table = table_name.split(".")
                for column_name, default_value in columns:
                    update_query = f"ALTER TABLE {schema}.{table} ALTER COLUMN {column_name} SET DEFAULT {default_value};"
                    cur.execute(update_query)

    def recreate_table_srid(self, schema_name="toposource", primary_key_type="int"):
        """Reproject all schema tables from EPSG:2193 to EPSG:4167.

        For each table registered with SRID 2193 the method creates a
        ``{table}_4167`` copy using ``ST_Transform``, drops the original,
        renames the copy, updates the geometry type metadata, and rebuilds
        spatial and attribute indexes.

        Args:
            schema_name: Schema containing the tables to reproject.
            primary_key_type: Primary key strategy (``'int'`` or ``'uuid'``)
                used when ordering columns for the recreated table.
        """
        self.connect()
        schema_tables = self.list_schema_tables(schema_name)

        for schema, tables in schema_tables.items():
            for table in tables:
                if table == "collections" or table == "nz_topo50_map_sheet":
                    continue

                # if table == "contour":
                #     continue

                fields = self.get_ordered_columns(schema, table, primary_key_type)
                if self.get_srid(schema, table) == 2193:
                    geom_field = "ST_Transform(geometry, 4167) AS geometry"

                    geom_type = self.get_geometry_type(schema, table)
                    # if creating an empty model geom_type will be None - infer from table name suffix
                    if geom_type is None:
                        # Fallback inference for empty tables by common naming conventions.
                        if table.endswith(("_line", "_crossing")):
                            geom_type = "LINESTRING"
                        elif table.endswith(
                            ("_point", "_text", "_name", "_station", "_locations")
                        ):
                            geom_type = "POINT"
                        elif table.endswith(("_sheet", "_area")):
                            geom_type = "POLYGON"
                        else:
                            geom_type = "POLYGON"

                    if not self.table_exists(schema, f"{table}_4167"):
                        create_query = f"""
                            CREATE TABLE "{schema}"."{table}_4167" AS
                            SELECT {", ".join([f'"{field}"' for field in fields])},
                                {geom_field} 
                            FROM "{schema}"."{table}";
                        """

                        with self.conn.cursor() as cur:
                            cur.execute(create_query)
                            self.conn.commit()
                            print(
                                f"Created table '{schema}.{table}_4167' with SRID 4167"
                            )

                    # Drop the original table and rename the new table
                    with self.conn.cursor() as cur:
                        drop_query = f'DROP TABLE "{schema}"."{table}" CASCADE;'
                        rename_query = f'ALTER TABLE "{schema}"."{table}_4167" RENAME TO "{table}";'
                        cur.execute(drop_query)
                        cur.execute(rename_query)
                        self.conn.commit()
                        print(
                            f"Dropped original table '{schema}.{table}' and renamed '{schema}.{table}_4167' to '{schema}.{table}'"
                        )

                    # update the SRID information
                    with self.conn.cursor() as cur:
                        query = f'ALTER TABLE "{schema}"."{table}" ALTER COLUMN geometry TYPE geometry({geom_type}, 4167) USING ST_SetSRID(geometry, 4167);'

                        cur.execute(query)
                        self.conn.commit()
                        print(f"Set SRID for table '{schema}.{table}' to 4167")

                    # Add indexes
                    index_sql = f"CREATE INDEX IF NOT EXISTS idx_{table}_geom ON {schema}.{table} USING GIST (geometry);"
                    with self.conn.cursor() as cur:
                        try:
                            cur.execute(index_sql)
                            self.conn.commit()
                        except Exception as e:
                            print(f"Error creating index for '{table}': {e}")
                            continue
                    print(f"Index for '{table}' created successfully.")

                    index_fields = ["use", "type", "name"]
                    for field_name in index_fields:
                        columns = self.column_list(schema, table, field_name)
                        if columns:
                            for field in columns:
                                if field == "change_type":
                                    continue
                                sql = f"CREATE INDEX IF NOT EXISTS idx_{table}_{field_name} ON {schema}.{table}({field});"
                                with self.conn.cursor() as cur:
                                    try:
                                        cur.execute(sql)
                                    except Exception as e:
                                        print(
                                            f"Error creating index for '{table}': {e}"
                                        )
                                        continue

    def add_name_columns(self):
        """Add a ``name`` column to tables that represent named features.

        Applies only to tables in the hard-coded list that do not already have
        the column.
        """
        self.connect()
        table_list = [
            "physical_infrastructure_point",
            "physical_infrastructure_line",
            "structure",
            # "vegetation",
            "landcover",
            "landcover_line",
            "ferry_crossing",
        ]

        for table in table_list:
            schema = self.table_schema(table)[0]
            if not self.column_exists(schema, table, "name"):
                self.add_column(f'"{schema}"."{table}"', "name", "VARCHAR(50)")

    def add_collectionid_columns(self):
        """Add a ``collection_id`` UUID column to tables that support grouping.

        Applies only to tables in the hard-coded list that do not already have
        the column.
        """
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
            if not self.column_exists(schema, table, "collection_id"):
                self.add_column(
                    f'"{schema}"."{table}"', "collection_id", "uuid"
                )
            # if not tableModifer.column_exists(schema, table, "collection_name"):
            #    tableModifer.add_column(f'"{schema}"."{table}"', "collection_name", "VARCHAR(100)")

    def create_collections_table(self, schema_name="toposource"):
        """Create the ``collections`` lookup table if it does not already exist.

        Args:
            schema_name: Schema in which the table is created.
        """
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
            print(
                f'Created table "{schema_name}.collections" with columns collection_id (uuid, primary key) and topo_id (uuid)'
            )

    def rename_columns(self, schema, table, old_column_name, new_column_name):
        """Rename a column if it exists.

        Args:
            schema: Schema name.
            table: Table name.
            old_column_name: Current column name.
            new_column_name: Replacement column name.
        """
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
        """Copy non-null values from one column into another then drop the source.

        Args:
            schema: Schema name.
            table: Table name.
            base_column_name: Destination column to receive values.
            drop_column_name: Source column to copy from and then remove.
        """
        self.connect()
        with self.conn.cursor() as cur:
            if self.column_exists(
                schema, table, base_column_name
            ) and self.column_exists(schema, table, drop_column_name):
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

    def drop_column(self, schema, table, drop_column_name):
        """Drop a column from a table if it exists.

        Args:
            schema: Schema name.
            table: Table name.
            drop_column_name: Column to remove.
        """
        self.connect()
        with self.conn.cursor() as cur:
            if self.column_exists(schema, table, drop_column_name):
                drop_query = (
                    f"ALTER TABLE {schema}.{table} DROP COLUMN {drop_column_name};"
                )
                cur.execute(drop_query)

                print(
                    f"Dropped column '{drop_column_name}' from table '{schema}.{table}'"
                )
            else:
                print(
                    f"Column '{drop_column_name}' does not exist in table '{schema}.{table}'"
                )

    def update_value_by_column(self, schema, table, column_name, from_column_name):
        """Set a column's value from another column or to NULL.

        Args:
            schema: Schema name.
            table: Table name.
            column_name: Destination column to update.
            from_column_name: Source column name, or ``'null'`` (case
                insensitive) to set all rows to NULL.
        """
        self.connect()
        with self.conn.cursor() as cur:
            if self.column_exists(schema, table, column_name):
                process = True
                if from_column_name.lower() == "null":
                    update_query = f"UPDATE {schema}.{table} SET {column_name} = NULL;"
                elif self.column_exists(schema, table, from_column_name):
                    update_query = f"UPDATE {schema}.{table} SET {column_name} = {from_column_name};"
                else:
                    print(
                        f"Column '{from_column_name}' does not exist in table '{schema}.{table}'"
                    )
                    process = False
                if process:
                    cur.execute(update_query)
                    self.conn.commit()
                    print(
                        f"Set value of column '{column_name}' to '{from_column_name}' in table '{schema}.{table}'"
                    )
            else:
                print(
                    f"Column '{column_name}' does not exist in table '{schema}.{table}'"
                )

    def update_column_by_value(self, schema, table, column_name, value):
        """Set every row in a column to a fixed value.

        String values that are not already quoted will have single quotes
        added automatically.

        Args:
            schema: Schema name.
            table: Table name.
            column_name: Column to update.
            value: Python value to assign. Bare strings are quoted before
                being used in SQL.
        """
        self.connect()
        with self.conn.cursor() as cur:
            if self.column_exists(schema, table, column_name):
                if isinstance(value, str) and not value.startswith("'"):
                    value = f"'{value}'"
                update_query = f"UPDATE {schema}.{table} SET {column_name} = {value};"

                cur.execute(update_query)
                self.conn.commit()
                print(
                    f"Set value of column '{column_name}' to '{value}' in table '{schema}.{table}'"
                )
            else:
                print(
                    f"Column '{column_name}' does not exist in table '{schema}.{table}'"
                )

    def all_ordered_columns(self, primary_key_type="int"):
        """Return the canonical column order for Topo50 tables.

        Args:
            primary_key_type: ``'uuid'`` repositions ``topo_id`` to index 0
                and removes the integer ``id`` column; any other value keeps
                the full ordered list as-is.

        Returns:
            list[str]: Ordered column names.
        """
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
            "edition",
            "example_class",
            "example_name",
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
            "version",
        ]

        if primary_key_type == "uuid":
            ordered_list.remove("id")
            ordered_list.remove("topo_id")
            ordered_list.insert(0, "topo_id")
        return ordered_list

    def get_ordered_columns(self, schema, table, primary_key_type="int"):
        """
        Returns a list of columns in the specified schema and table, ordered according to the predefined order.
        """
        self.connect()
        ordered_columns = self.all_ordered_columns(primary_key_type)
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """,
                (schema, table),
            )
            existing_columns = [row[0] for row in cur.fetchall()]

        # Filter and order the existing columns according to the predefined order
        return [col for col in ordered_columns if col in existing_columns]


class TableModificationWorkflow:
    """Orchestrates release-specific table modification steps."""

    def __init__(
        self,
        db_params,
        schema_name,
        option="all",
        add_full_metadata_fields=True,
        primary_key_type="uuid",
        release_date=None,
    ):
        self.table_modifer = ModifyTable(db_params)
        self.schema_name = schema_name
        self.option = option
        self.add_full_metadata_fields = add_full_metadata_fields
        self.primary_key_type = primary_key_type
        self.release_date = release_date

    def should_run(self, step_name):
        """Return True when a step should run for the current option."""
        return self.option in ("all", step_name)

    def step_metadata(self):
        self.table_modifer.add_metadata_columns(
            mode="add",
            schema_name=self.schema_name,
            full_field_set=self.add_full_metadata_fields,
        )

    def step_columns(self):
        update_dict = {
            (self.schema_name, "structure_point"): [
                ("structure_use", "shaft_use"),
                ("structure_type", "tank_type"),
                ("material", "materials"),
            ],
            (self.schema_name, "structure_line"): [("status", "dam_status")],
            (self.schema_name, "structure"): [
                ("species", "species_cultivated"),
                ("lid_type", "reservoir_lid_type"),
                ("structure_type", "tank_type"),
            ],
            (self.schema_name, "road_line"): [("highway_number", "highway_numb")],
            (self.schema_name, "water"): [
                ("water_use", "pond_use"),
                ("height", "elevation"),
            ],
        }

        for (schema, table), columns in update_dict.items():
            for base_col, drop_col in columns:
                self.table_modifer.set_base_column_and_drop_column(
                    schema, table, base_col, drop_col
                )
        self.table_modifer.drop_column(self.schema_name, "tree_locations", "name")

        # specific updates
        self.table_modifer.update_column_with_default(
            self.schema_name,
            "tunnel_line",
            "tunnel_use2",
            "'livestock'",
            "tunnel_use2 ='ivestock'",
        )
        self.table_modifer.update_column_with_default(
            self.schema_name,
            "tunnel_line",
            "tunnel_use",
            "'vehicle'",
            "tunnel_use2 ='vehicle'",
        )
        self.table_modifer.update_column_with_default(
            self.schema_name,
            "tunnel_line",
            "tunnel_use2",
            "'livestock'",
            "tunnel_use2 ='vehicle'",
        )

        self.table_modifer.update_column_with_default(
            self.schema_name, "trig_point", "trig_type", "'beacon'"
        )

        self.table_modifer.update_column_with_default(
            self.schema_name, "road_line", "way_count", "'one way'", "way_count ='1'"
        )
        if self.table_modifer.column_exists(self.schema_name, "road_line", "road_access"):
            self.table_modifer.update_column_with_default(
                self.schema_name, "road_line", "road_access", "'mp'", "road_access ='m'"
            )

        self.table_modifer.update_column_with_default(
            self.schema_name,
            "physical_infrastructure_line",
            "support_type",
            "'pole'",
            "feature_type ='telephone'",
        )

    def step_name(self):
        self.table_modifer.add_name_columns()
        # self.table_modifer.add_collectionid_columns()

    def step_null_updates(self):
        self.table_modifer.populate_defined_null_values(self.schema_name)

    def step_additions(self):
        self.table_modifer.add_column(
            f"{self.schema_name}.trig_point", "code", "VARCHAR(20)"
        )
        self.table_modifer.update_value_by_column(
            self.schema_name, "trig_point", "code", "name"
        )
        self.table_modifer.update_value_by_column(
            self.schema_name, "trig_point", "name", "null"
        )

        self.table_modifer.add_column(
            f"{self.schema_name}.vegetation", "sub_type", "VARCHAR(50)"
        )
        self.table_modifer.update_value_by_column(
            self.schema_name, "vegetation", "sub_type", "species"
        )
        self.table_modifer.update_value_by_column(
            self.schema_name, "vegetation", "species", "null"
        )

        self.table_modifer.add_column(
            f"{self.schema_name}.landcover", "sub_type", "VARCHAR(50)"
        )

        # self.table_modifer.add_column(f"{self.schema_name}.road_line", "level", "INTEGER")
        # self.table_modifer.update_column_by_value(
        #     self.schema_name, "road_line", "level", 0
        # )
        self.table_modifer.add_column(
            f"{self.schema_name}.road_line", "hierarchy", "VARCHAR(50)"
        )

        self.table_modifer.add_column(
            f"{self.schema_name}.descriptive_text", "nzgb_id", "BIGINT"
        )
        self.table_modifer.add_column(
            f"{self.schema_name}.railway_line", "nzgb_id", "BIGINT"
        )
        self.table_modifer.add_column(
            f"{self.schema_name}.railway_line", "route", "VARCHAR(30)"
        )
        self.table_modifer.add_column(
            f"{self.schema_name}.railway_line", "route2", "VARCHAR(30)"
        )
        self.table_modifer.add_column(
            f"{self.schema_name}.railway_line", "route3", "VARCHAR(30)"
        )

        self.table_modifer.add_column(
            f"{self.schema_name}.coastline", "coastline_type", "VARCHAR(50)"
        )

        self.table_modifer.add_column(
            f"{self.schema_name}.road_line", "hierarchy", "VARCHAR(25)"
        )
        # self.table_modifer.add_column(f"{self.schema_name}.river_line", "hierarchy", "VARCHAR(25)")
        # self.table_modifer.add_column(f"{self.schema_name}.river", "hierarchy", "VARCHAR(25)")
        self.table_modifer.add_column(
            f"{self.schema_name}.water_line", "hierarchy", "VARCHAR(25)"
        )
        self.table_modifer.add_column(
            f"{self.schema_name}.water", "hierarchy", "VARCHAR(25)"
        )

        self.table_modifer.rename_columns(
            self.schema_name, "contour", "nat_form", "formation"
        )
        self.table_modifer.rename_columns(
            self.schema_name, "contour", "designated", "designation"
        )

        self.table_modifer.rename_columns(
            self.schema_name, "landuse", "track_type", "landuse_type"
        )
        self.table_modifer.update_value_by_column(
            self.schema_name, "landuse", "landuse_type", "visibility"
        )
        self.table_modifer.drop_column(self.schema_name, "landuse", "visibility")
        self.table_modifer.rename_columns(
            self.schema_name, "landuse_line", "track_type", "landuse_type"
        )
        self.table_modifer.rename_columns(
            self.schema_name, "place_point", "visibility", "place_type"
        )

        # offshore (1) or inland island (0) - added manually using sea_coastline poly shapefile create from coastline and box
        # self.table_modifer.add_column(f"{self.schema_name}.island", "location", "INTEGER")

    def step_defaults(self):
        self.table_modifer.set_default_values(self.schema_name)

    def step_rename(self):
        rename_dict = {
            f"{self.schema_name}.structure_line": [
                ("materials", "material"),
                ("mtlconveyd", "material_conveyed"),
            ],
            f"{self.schema_name}.structure_point": [("store_item", "stored_item")],
            f"{self.schema_name}.structure": [("store_item", "stored_item")],
        }

        for table_full, columns in rename_dict.items():
            schema, table = table_full.split(".")
            for old_column_name, new_column_name in columns:
                self.table_modifer.rename_columns(
                    schema, table, old_column_name, new_column_name
                )

    def step_carto_text_geom_update(self):
        self.table_modifer.carto_text_geom_update(self.schema_name, "nz_topo50_map_sheet")

    def step_recreate_table_srid(self):
        self.table_modifer.recreate_table_srid(
            self.schema_name, self.primary_key_type
        )
        self.table_modifer.add_metadata_columns(
            "alter", self.schema_name, self.add_full_metadata_fields
        )

    def step_primary_key(self):
        schema_tables = self.table_modifer.list_schema_tables(self.schema_name)
        for schema, tables in schema_tables.items():
            if self.primary_key_type == "int":
                for table in tables:
                    # all tables processed from any esri fields
                    self.table_modifer.drop_column(schema, table, "ESRI_OID")
                    self.table_modifer.update_primary_key(schema, table, "id")
            else:
                for table in tables:
                    # all tables processed from any esri fields
                    self.table_modifer.drop_column(schema, table, "ESRI_OID")
                    self.table_modifer.update_primary_key_guid(schema, table, "topo_id")

    # turn the concept of collections off for now as no direct requirement
    # def step_collections(self):
    #     self.table_modifer.create_collections_table(self.schema_name)

    def step_process_carto_tables(self):
        tables_to_copy = ["nz_topo50_map_sheet"]
        for table_name in tables_to_copy:
            # Check if table exists in source schema
            if self.table_modifer.table_exists(self.schema_name, table_name):
                # Check if table exists in carto schema and delete it
                if self.table_modifer.table_exists("carto", table_name):
                    with self.table_modifer.conn.cursor() as cur:
                        drop_query = f'DROP TABLE "carto"."{table_name}" CASCADE;'
                        cur.execute(drop_query)
                        self.table_modifer.conn.commit()
                        print(f"Dropped existing table 'carto.{table_name}'")

                # Copy table from source schema to carto schema
                with self.table_modifer.conn.cursor() as cur:
                    copy_query = f'CREATE TABLE "carto"."{table_name}" AS SELECT * FROM "{self.schema_name}"."{table_name}";'
                    cur.execute(copy_query)
                    self.table_modifer.conn.commit()
                    # Add index on topo_id field if it exists
                    if self.table_modifer.column_exists("carto", table_name, "topo_id"):
                        index_sql = f"CREATE INDEX IF NOT EXISTS idx_{table_name}_topo_id ON carto.{table_name} (topo_id);"
                        cur.execute(index_sql)
                        self.table_modifer.conn.commit()
                        print(f"Created index on topo_id for 'carto.{table_name}'")
                    print(
                        f"Copied table '{self.schema_name}.{table_name}' to 'carto.{table_name}'"
                    )
            else:
                print(
                    f"Table '{self.schema_name}.{table_name}' does not exist in source schema"
                )

    def run(self):
        """Run selected workflow steps in the original execution order."""
        steps = [
            ("metadata", self.step_metadata),
            ("columns", self.step_columns),
            ("name", self.step_name),
            ("null_updates", self.step_null_updates),
            ("additions", self.step_additions),
            ("defaults", self.step_defaults),
            ("rename", self.step_rename),
            ("carto_text_geom_update", self.step_carto_text_geom_update),
            ("recreate_table_srid", self.step_recreate_table_srid),
            ("primary_key", self.step_primary_key),
            ("process_carto_tables", self.step_process_carto_tables),
        ]

        for step_name, step_func in steps:
            if self.should_run(step_name):
                step_func()


if __name__ == "__main__":
    option = "all"
    # option = "compare"
    # schema_name = "toposource"
    schema_name = "release64"
    schema_name = "model"
    release_date = "2025-09-25"

    # schema_name = "release62"
    # release_date = "2024-11-15"

    add_full_metadata_fields = True
    # primary_key_type = "none"
    # primary_key_type = 'int'
    primary_key_type = "uuid"

    workflow = TableModificationWorkflow(
        DB_PARAMS,
        schema_name=schema_name,
        option=option,
        add_full_metadata_fields=add_full_metadata_fields,
        primary_key_type=primary_key_type,
        release_date=release_date,
    )
    workflow.run()
