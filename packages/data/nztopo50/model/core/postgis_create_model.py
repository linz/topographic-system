from typing import Any
import psycopg
import pandas as pd

# Database connection parameters
db_params: dict[str, Any] = {
    "dbname": "topo",
    "user": "postgres",
    "password": "landinformation",
    "host": "localhost",
    "port": 5432,
}


def excel_to_layered_dict(excel_path):
    df = pd.read_excel(excel_path)
    layered_dict = {}

    for _, row in df.iterrows():
        dataset, layer, name, mapped_name, fieldtype, length = row
        dataset = dataset.replace("_Layers", "")
        layered_dict.setdefault(dataset, {})
        layered_dict[dataset].setdefault(layer, {})
        layered_dict[dataset][layer][mapped_name] = {
            "type": fieldtype,
            "length": int(length),
        }

    return layered_dict


if __name__ == "__main__":
    commands_options = ["drop_tables", "create_tables"]
    #commands_options = ["drop_tables"]
    commands_options = ["create_tables"]
    # commands_options = ["create_indexes"]
    for command_option in commands_options:
        print(f"Executing command: {command_option}")

        model_fields_file = r"C:\Data\Model\datasets_fields.xlsx"
        # schema_name = "toposource"
        # schema_name = "release62"
        schema_name = "release64"

        primary_key_type = "none"
        # primary_key_type = 'int' #uuid
        # primary_key_type = "uuid"

        layered_dict = excel_to_layered_dict(model_fields_file)

        for layers in layered_dict.items():
            for layer, fields in layers[1].items():
                geom = fields["SHAPE"]["type"]
                if geom.lower() == "polyline":
                    geom = "LINESTRING"

                table = layer.lower()
                # TEMP - only create one named table 
                # if table != "contour":
                #     continue
                if primary_key_type == "int":
                    columns = ["id SERIAL PRIMARY KEY"]
                elif primary_key_type == "uuid":
                    columns = ["topo_id uuid PRIMARY KEY DEFAULT gen_random_uuid()"]
                else:
                    columns = ["topo_id uuid DEFAULT gen_random_uuid()"]
                for field_name, props in fields.items():
                    if field_name.lower() in ["shape", "objectid", "shape_length"]:
                        continue
                    else:
                        col_type = props["type"].upper()
                        if col_type == "STRING":
                            col_type = "VARCHAR"

                        if col_type == "VARCHAR":
                            col_def = f"{field_name} VARCHAR({props['length']})"
                        elif col_type in ("INTEGER", "INT"):
                            col_def = f"{field_name} INTEGER"
                        elif col_type == "BIGINTEGER":
                            col_def = f"{field_name} BIGINT "
                        elif col_type == "FLOAT":
                            col_def = f"{field_name} FLOAT"
                        elif col_type == "DOUBLE":
                            col_def = f"{field_name} DOUBLE PRECISION"
                        elif col_type == "DATE":
                            col_def = f"{field_name} DATE"
                        elif col_type == "BOOLEAN":
                            col_def = f"{field_name} BOOLEAN"
                        elif col_type == "GUID":
                            if (
                                primary_key_type == "guid"
                                and field_name.lower() == "topo_id"
                            ):
                                continue
                            else:
                                col_def = f"{field_name} uuid DEFAULT gen_random_uuid()"
                        else:
                            col_def = f"{field_name} TEXT"
                        columns.append(col_def)

                columns.append(f"geometry geometry({geom}, 2193)")

                if command_option == "create_tables":
                    print(f"Creating table '{table}'.")
                    create_sql = (
                        f"CREATE TABLE IF NOT EXISTS {schema_name}.{table} (\n    "
                        + ",\n    ".join(columns)
                        + "\n);"
                    )

                    with psycopg.connect(**db_params) as conn:
                        with conn.cursor() as cur:
                            try:
                                cur.execute(create_sql)
                                conn.commit()
                            except Exception:
                                print(
                                    "likely error postgis extension not installed in schema - CREATE EXTENSION postgis; "
                                )
                                print(create_sql)
                                continue

                if command_option == "drop_tables":
                    print(f"Dropping table '{table}' in schema '{schema_name}'...")
                    create_sql = f"DROP TABLE IF EXISTS {schema_name}.{table} CASCADE;"

                    with psycopg.connect(**db_params) as conn:
                        with conn.cursor() as cur:
                            try:
                                cur.execute(create_sql)
                                conn.commit()
                            except Exception:
                                print(create_sql)
                                continue

                if table == "collections":
                    continue

                if command_option == "create_indexes":
                    index_sql = f"CREATE INDEX IF NOT EXISTS idx_{table}_geom ON {schema_name}.{table} USING GIST (geometry);"
                    with psycopg.connect(**db_params) as conn:
                        with conn.cursor() as cur:
                            try:
                                cur.execute(index_sql)
                                conn.commit()
                            except Exception as e:
                                print(f"Error creating index for '{table}': {e}")
                                continue
                    print(f"Index for '{table}' created successfully.")

                    index_sql = f"CREATE INDEX IF NOT EXISTS idx_{table}_class ON {schema_name}.{table}(feature_type);"
                    with psycopg.connect(**db_params) as conn:
                        with conn.cursor() as cur:
                            try:
                                cur.execute(index_sql)
                                conn.commit()
                            except Exception as e:
                                print(f"Error creating index for '{table}': {e}")
                                continue
                    print(f"Index for '{table}' created successfully.")

                    index_sql = f"CREATE INDEX IF NOT EXISTS idx_{table}_class ON {schema_name}.{table}(t50_fid);"
                    with psycopg.connect(**db_params) as conn:
                        with conn.cursor() as cur:
                            try:
                                cur.execute(index_sql)
                                conn.commit()
                            except Exception as e:
                                print(f"Error creating index for '{table}': {e}")
                                continue
                    print(f"Index for '{table}' created successfully.")
