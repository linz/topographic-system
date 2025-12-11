import os
import json
import time
from validate_dataset import TopologyValidator, TopoValidatorTools


def options_layer_postgres_where():
    layers = [
        {
            "table": "topoedit.building",
            "layername": "building-validation",
            "where": "auto_pk in (65052, 65053, 65054)",
        }
    ]
    return layers


def build_where_statement(active_dict):
    where = active_dict.get("where", None)
    date = active_dict.get("date", None)
    weeks = active_dict.get("weeks", None)
    datetool = TopoValidatorTools()

    if date is not None:
        date_where = datetool.get_update_date(date=date)
        if where is not None:
            where = f"({where}) AND ({date_where})"
        else:
            where = date_where

    elif weeks is not None:
        date_where = datetool.get_update_date(date=None, weeks=weeks)
        if where is not None:
            where = f"({where}) AND ({date_where})"
        else:
            where = date_where

    return where


if __name__ == "__main__":
    mode = "generic"

    if mode == "postgis":
        # set data source and read configuration - postgres
        validation_config_file = "./validation/src/validation_postgis_config.json"
        db_url = "postgresql://postgres:landinformation@localhost:5432/topo"
    else:
        # set data source and read configuration - gkpg and parquet
        validation_config_file = "./validation/src/validation_generic_config.json"
        db_url = r"C:\Data\topoedit\topographic-data\topographic-data.gpkg"
        # db_url = r"C:\Data\temp\2025-02-05\files.parquet"

    with open(validation_config_file, "r") as f:
        loaded_data = json.load(f)
        feature_not_on_layers = loaded_data["feature_not_on_layers"]
        feature_in_layers = loaded_data["feature_in_layers"]
        line_not_on_feature_layers = loaded_data["line_not_on_feature_layers"]
        line_not_touches_feature_layers = loaded_data["line_not_touches_feature_layers"]
        feature_not_contains_layers = loaded_data["feature_not_contains_layers"]
        self_intersect_layers = loaded_data["self_intersect_layers"]
        null_columns = loaded_data["null_columns"]
        query_rules = loaded_data["query_rules"]

    output_dir = r"C:\Data\topoedit\validation-data"
    output_dir = r"C:\temp\validation-data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    area_crs = 2193
    export_parquet = False
    export_parquet_by_geometry_type = False
    export_gpkg = True
    use_date_folder = False
    process_queries = False
    process_features_on_layer = False
    process_self_intersections = False
    process_multipart_geometries = True

    folders = TopoValidatorTools()
    output_dir = folders.prep_output_folder(output_dir, use_date=use_date_folder)
    all_processes_start_time = time.time()
    print("Starting validation run...")

    if process_queries:
        for null_check in null_columns:
            validator = TopologyValidator(
                db_url=db_url,
                table=null_check["table"],
                export_layername=null_check["table"],
                where_condition=build_where_statement(null_check),
                message=null_check.get("message"),
                output_dir=output_dir,
                area_crs=area_crs,
            )
            validator.set_exports(
                export_parquet=export_parquet,
                export_parquet_by_geometry_type=export_parquet_by_geometry_type,
                export_gpkg=export_gpkg,
            )
            validator.run_null_column_checks(column_name=null_check["column"])

        for query_rule in query_rules:
            validator = TopologyValidator(
                db_url=db_url,
                table=query_rule["table"],
                export_layername=query_rule["table"],
                where_condition=build_where_statement(query_rule),
                message=query_rule.get("message"),
                output_dir=output_dir,
                area_crs=area_crs,
            )
            validator.set_exports(
                export_parquet=export_parquet,
                export_parquet_by_geometry_type=export_parquet_by_geometry_type,
                export_gpkg=export_gpkg,
            )
            validator.run_query_rule_checks(
                rule=query_rule["rule"], column_name=query_rule["column"]
            )

    if process_features_on_layer:
        for layer in feature_in_layers:
            validator = TopologyValidator(
                db_url=db_url,
                table=layer["table"],
                table2=layer["intersection_table"],
                export_layername=layer["layername"],
                where_condition=build_where_statement(layer),
                message=layer["message"],
                output_dir=output_dir,
                area_crs=area_crs,
            )
            validator.set_exports(
                export_parquet=export_parquet,
                export_parquet_by_geometry_type=export_parquet_by_geometry_type,
                export_gpkg=export_gpkg,
            )
            validator.run_layer_intersections()

        for layer in feature_not_on_layers:
            validator = TopologyValidator(
                db_url=db_url,
                table=layer["table"],
                table2=layer["intersection_table"],
                export_layername=layer["layername"],
                where_condition=build_where_statement(layer),
                message=layer["message"],
                output_dir=output_dir,
                area_crs=area_crs,
            )
            validator.set_exports(
                export_parquet=export_parquet,
                export_parquet_by_geometry_type=export_parquet_by_geometry_type,
                export_gpkg=export_gpkg,
            )
            validator.run_layer_intersections(intersect=False)

        for layer in line_not_on_feature_layers:
            validator = TopologyValidator(
                db_url=db_url,
                table=layer["line_table"],
                table2=layer["intersection_table"],
                export_layername=layer["layername"],
                where_condition=build_where_statement(layer),
                message=layer["message"],
                output_dir=output_dir,
                area_crs=area_crs,
            )
            validator.set_exports(
                export_parquet=export_parquet,
                export_parquet_by_geometry_type=export_parquet_by_geometry_type,
                export_gpkg=export_gpkg,
            )
            validator.run_layer_intersections(intersect=False, buffer_lines=True)

        for layer in line_not_touches_feature_layers:
            validator = TopologyValidator(
                db_url=db_url,
                table=layer["line_table"],
                table2=layer["intersection_table"],
                export_layername=layer["layername"],
                where_condition=build_where_statement(layer),
                message=layer["message"],
                output_dir=output_dir,
                area_crs=area_crs,
            )
            validator.set_exports(
                export_parquet=export_parquet,
                export_parquet_by_geometry_type=export_parquet_by_geometry_type,
                export_gpkg=export_gpkg,
            )
            validator.run_layer_intersections(
                intersect=False, buffer_lines=True, predicate="touches"
            )

        for layer in feature_not_contains_layers:
            validator = TopologyValidator(
                db_url=db_url,
                table=layer["table"],
                table2=layer["intersection_table"],
                export_layername=layer["layername"],
                where_condition=build_where_statement(layer),
                message=layer["message"],
                output_dir=output_dir,
                area_crs=area_crs,
            )
            validator.set_exports(
                export_parquet=export_parquet,
                export_parquet_by_geometry_type=export_parquet_by_geometry_type,
                export_gpkg=export_gpkg,
            )
            validator.run_layer_intersections(
                intersect=False, buffer_lines=False, predicate="contains"
            )

    if process_self_intersections:
        for layer in self_intersect_layers:
            validator = TopologyValidator(
                db_url=db_url,
                table=layer["table"],
                export_layername=layer["layername"],
                where_condition=build_where_statement(layer),
                message=layer.get("message"),
                output_dir=output_dir,
                area_crs=area_crs,
            )
            validator.set_exports(
                export_parquet=export_parquet,
                export_parquet_by_geometry_type=export_parquet_by_geometry_type,
                export_gpkg=export_gpkg,
            )
            validator.run_self_intersections()

    if process_multipart_geometries:
        for layer in loaded_data["is_multipart"]:
            validator = TopologyValidator(
                db_url=db_url,
                table=layer["table"],
                export_layername=layer["table"],
                where_condition=build_where_statement(layer),
                message=layer.get("message"),
                output_dir=output_dir,
                area_crs=area_crs,
            )
            validator.set_exports(
                export_parquet=export_parquet,
                export_parquet_by_geometry_type=export_parquet_by_geometry_type,
                export_gpkg=export_gpkg,
            )
            validator.run_multipart_geometry_checks()
    seconds = time.time() - all_processes_start_time
    minutes = seconds / 60
    print(
        f"All processes completed. Total time taken: {seconds:.2f} seconds ({minutes:.2f} minutes)"
    )
