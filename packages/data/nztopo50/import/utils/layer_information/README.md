# layer_info

Utility scripts for generating schemaand helper outputs describing the topo50 dataset information.

## Script notes

### Step 1a: process_layer_info.py

Summary: reads the layer_info.xsls file writes out the theme and layer names.


### Step 1b: postgis_list_schema_tables.py

Summary: output schema table information.

- Connects to Postgres and lists tables for the configured schema.
- Exports schema/table row counts to `schema_tables.csv`.
- Exports schema/table/column mappings to `schema_columns.csv`.
- Exports distinct column names to `columns.csv`.
- Exports feature type summaries to CSV outputs by table, theme, and column.
- Writes helper Kart import command files (`kart_import.txt`,
  `kart_import.bat`).

### Step 2a: create_groupby_csv.py 

Generates a list of columns to group by - hard coded list. Used in sql steps.

### Step 2b: create_sqlstatements_column_contents.py 

Generates the base SQL query file used to inspect grouped column contents.

- Reads table/column metadata from `schema_columns_groupby.csv`.
- Keeps only rows where `group_by` is true-like (`TRUE`, `T`, `1`, `YES`, `Y`).
- Builds `SELECT type, <column>` queries per table/column.
- Adds `GROUP BY type, <column>` and `ORDER BY <column>` clauses.
- Writes all generated SQL statements to `schema_columns_groupby_queries.sql`.


### Step 2c: create_sqlstatements_name.py

Builds SQL checks for NAME fields using rows from
`schema_columns_groupby.csv`.

- Reads table/column metadata from the CSV file.
- Keeps only rows where the column is `NAME`.
- Writes SQL statements to `schema_columns_groupby_name_queries.sql`.
- Generated SQL selects `type` and `NAME`, filters out NULL `NAME`
	values, and groups/orders results for easy inspection.

### Step 2d: run_sqlstatements.py

Executes SQL statements from `schema_columns_groupby_queries.sql` and exports
query results to CSV files.

- Loads semicolon-separated SQL statements from the input SQL file.
- Connects to Postgres using the configured `DB_PARAMS` values.
- Optionally sets the schema search path before running statements.
- Writes one CSV per statement in `sql_results`, including column headers.
- Generates readable CSV filenames from the `SELECT` list and `FROM` table,
  with indexed fallback names if parsing is not possible.



