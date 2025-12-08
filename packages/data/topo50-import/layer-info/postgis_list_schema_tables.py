import psycopg
import csv

# Database connection parameters
DB_PARAMS = {
    "dbname": "topo",
    "user": "postgres",
    "password": "landinformation",
    "host": "localhost",
    "port": 5432,
}


def list_schema_tables(db_params, schema="toposource"):
    query = f"""
    SELECT table_schema, table_name FROM information_schema.tables
    WHERE table_schema = '{schema}'
    AND table_name NOT IN ('_kart_state', '_kart_track')
    ORDER BY table_schema, table_name
    """
    try:
        with psycopg.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                schema_tables = {}
                for schema, table in rows:
                    if schema not in schema_tables:
                        schema_tables[schema] = []
                    schema_tables[schema].append(table)
        return schema_tables
    except Exception:
        return {}


def list_columns(db_params, schema="toposource"):
    query = f"""
    SELECT DISTINCT column_name FROM information_schema.columns
    WHERE table_schema = '{schema}'
    ORDER BY column_name
    """
    try:
        with psycopg.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                columns = []
                for column in rows:
                    columns.append(column[0])
        return columns
    except Exception:
        return []


def list_schema_table_columns(db_params, schema="toposource"):
    query = f"""
    SELECT table_schema, table_name, column_name FROM information_schema.columns
    WHERE table_schema = '{schema}'
    ORDER BY table_schema, table_name, ordinal_position
    """
    try:
        with psycopg.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                schema_table_columns = {}
                for schema, table, column in rows:
                    if schema not in schema_table_columns:
                        schema_table_columns[schema] = {}
                    if table not in schema_table_columns[schema]:
                        schema_table_columns[schema][table] = []
                    schema_table_columns[schema][table].append(column)
        return schema_table_columns
    except Exception:
        return {}


def get_table_row_count(db_params, schema, table):
    query = f'SELECT COUNT(*) FROM "{schema}"."{table}"'
    try:
        with psycopg.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                count = cur.fetchone()[0]
        return count
    except Exception:
        return 0


def get_unique_feature_types(db_params, schema, table):
    query = f"""
    SELECT DISTINCT feature_type FROM "{schema}"."{table}"
    ORDER BY feature_type
    """
    try:
        with psycopg.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                feature_types = [row[0] for row in rows]
        return feature_types
    except Exception:
        return []


if __name__ == "__main__":
    local_pwd = "landinformation"

    schema_name = "toposource"
    schema_name = "release62"

    file = r"C:\Data\model\schema_tables.csv"
    fileschemacolumns = r"C:\Data\model\schema_columns.csv"
    filecolumns = r"C:\Data\model\columns.csv"
    filekart = r"C:\Data\model\kart_import.txt"
    filekart_bat = r"C:\Data\model\kart_import.bat"
    fileschemafeatures = r"C:\Data\model\schema_features.csv"
    fileschemafeatures_bycolumn = r"C:\Data\model\schema_features_column.csv"

    schema_tables = list_schema_tables(DB_PARAMS, schema_name)
    schema_table_columns = list_schema_table_columns(DB_PARAMS, schema_name)
    columns_names = list_columns(DB_PARAMS, schema_name)

    with open(file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["schema", "table", "count"])
        for schema, tables in schema_tables.items():
            for table in tables:
                # bad - reopening connection for each table
                count = get_table_row_count(DB_PARAMS, schema, table)
                writer.writerow([schema, table, count])

    with open(fileschemacolumns, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["schema", "table", "columns"])
        for schema, tables in schema_tables.items():
            for table in tables:
                columns = schema_table_columns[schema][table]
                for column in columns:
                    writer.writerow([schema, table, column])

    with open(filecolumns, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["column_name"])
        for column in columns_names:
            writer.writerow([column])

    with open(filekart, "w", newline="", encoding="utf-8") as csvfile:
        for schema, tables in schema_tables.items():
            for table in tables:
                csvfile.write(
                    f"kart import postgresql://postgres:{local_pwd}@localhost/topo/{schema_name}  --primary-key topo_id {table}\n"
                )

    with open(filekart_bat, "w", newline="", encoding="utf-8") as csvfile:
        for schema, tables in schema_tables.items():
            for table in tables:
                csvfile.write(
                    f"kart import postgresql://postgres:{local_pwd}@localhost/topo/{schema_name}  --primary-key topo_id {table} --replace-existing\n"
                )
                csvfile.write("kart push origin master\n")

    if table.strip() == "collections":
        exit()

    with open(fileschemafeatures, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["schema", "table", "feature_type"])
        for schema, tables in schema_tables.items():
            for table in tables:
                feature_types = get_unique_feature_types(DB_PARAMS, schema, table)
                for feature_type in feature_types:
                    writer.writerow([schema, table, feature_type])

    with open(
        fileschemafeatures_bycolumn, "w", newline="", encoding="utf-8"
    ) as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["schema", "table", "column", "feature_type"])
        for schema, tables in schema_tables.items():
            for table in tables:
                feature_types = get_unique_feature_types(DB_PARAMS, schema, table)
                columns = schema_table_columns[schema][table]
                for column in columns:
                    for feature_type in feature_types:
                        writer.writerow([schema, table, column, feature_type])
