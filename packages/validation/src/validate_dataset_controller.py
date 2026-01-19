import time
import json
from topology_validator_factory import TopologyValidatorFactory
from topology_validator_tools import TopoValidatorSettings, TopoValidatorTools


class ValidateDatasetController:
    def __init__(self, settings: TopoValidatorSettings):
        self.settings = settings
        self.summary_report = self.default_validation_summary_dictionary()

    def default_validation_summary_dictionary(self):
        return {
            "feature_not_on_layers_about": "If True - a feature is found that does not lie on the specified layer.",
            "feature_not_on_layers": False,
            "feature_in_layers_about": "If True - a feature is found that does not lie within the specified layer.",
            "feature_in_layers": False,
            "line_not_on_feature_layers_about": "If True - a line is found that does not lie on the specified feature layer.",
            "line_not_on_feature_layers": False,
            "line_not_touches_feature_layers_about": "If True - a line is found that does not touch the specified feature layer.",
            "line_not_touches_feature_layers": False,
            "feature_not_contains_layers_about": "If True - a feature is found that does not contain the specified feature layer.",
            "feature_not_contains_layers": False,
            "self_intersect_layers_about": "If True - a feature is found that self-intersects.",
            "self_intersect_layers": False,
            "null_columns_about": "If True - a feature is found that has null values in the specified column.",
            "null_columns": False,
            "query_rule_about": "If True - a feature is found that meets the specified query rule.",
            "query_rules": False,
            "null_columns_all_about": "If True - a feature is found that has a null value for metadata columns.",
            "null_columns_all": False,
        }

    def write_summary_report(self, summary_report_file):
        with open(summary_report_file, "w") as f:
            json.dump(self.summary_report, f, indent=4)

    def build_where_statement(self, active_dict):
        # CHANGE TO SETTINGS
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

    def run_validation(self):
        folders = TopoValidatorTools()
        self.settings.output_dir = folders.prep_output_folder(
            output_dir=self.settings.output_dir, use_date=self.settings.use_date_folder
        )
        self.validator = TopologyValidatorFactory(self.settings)
        all_processes_start_time = time.time()

        if self.settings.process_queries:
            print("Processing query rules and null checks...")
            self.run_process_queries()

        if self.settings.process_features_on_layer:
            print("Processing features on layer checks...")
            self.run_process_features_on_layer()

        if self.settings.process_self_intersections:
            print("Processing self-intersection checks...")
            self.run_process_self_intersections()

        seconds = time.time() - all_processes_start_time
        minutes = seconds / 60
        msg = f"All processes completed. Total time taken: {seconds:.2f} seconds ({minutes:.2f} minutes)"
        self.summary_report["validation_completed_message"] = msg
        self.write_summary_report(
            summary_report_file=f"{self.settings.output_dir}/validation_summary_report.json"
        )
        print(msg)

    def run_process_queries(self):
        for null_check in self.settings.null_columns:
            table = null_check["table"]
            # export_layername = null_check["table"]
            where_condition = self.build_where_statement(null_check)
            message = null_check.get("message", "")

            print(f"Running null check on {table}, column: {where_condition}")

            validator = self.validator.create_validator(
                summary_report=self.summary_report,
                export_validation_data=self.settings.export_validation_data,
                table=table,
                where_condition=where_condition,
                message=message,
            )

            validator.set_exports(
                export_parquet=self.settings.export_parquet,
                export_parquet_by_geometry_type=self.settings.export_parquet_by_geometry_type,
                export_gpkg=self.settings.export_gpkg,
            )
            validator.run_null_column_checks(
                rule_name="null_columns", column_name=null_check["column"]
            )
            self.summary_report = validator.summary_report

        for query_rule in self.settings.query_rules:
            table = query_rule["table"]
            # export_layername = query_rule["table"]
            where_condition = self.build_where_statement(query_rule)
            message = query_rule.get("message", "")

            print(f"Running null check on {table}, column: {where_condition}")

            validator = self.validator.create_validator(
                summary_report=self.summary_report,
                export_validation_data=self.settings.export_validation_data,
                table=table,
                where_condition=where_condition,
                message=message,
            )

            validator.set_exports(
                export_parquet=self.settings.export_parquet,
                export_parquet_by_geometry_type=self.settings.export_parquet_by_geometry_type,
                export_gpkg=self.settings.export_gpkg,
            )

            validator.run_query_rule_checks(
                rule_name="query_rule",
                rule=query_rule["rule"],
                column_name=query_rule["column"],
            )
            self.summary_report = validator.summary_report

    def run_process_features_on_layer(self):
        for layer in self.settings.feature_in_layers:
            validator = self.validator.create_validator(
                summary_report=self.summary_report,
                export_validation_data=self.settings.export_validation_data,
                table=layer["table"],
                table2=layer["intersection_table"],
                export_layername=layer["layername"],
                where_condition=self.build_where_statement(layer),
                message=layer["message"],
            )

            print(
                f"Running feature on layer check between {layer['table']} and {layer['intersection_table']}"
            )

            validator.set_exports(
                export_parquet=self.settings.export_parquet,
                export_parquet_by_geometry_type=self.settings.export_parquet_by_geometry_type,
                export_gpkg=self.settings.export_gpkg,
            )
            validator.run_layer_intersections(
                rule_name="feature_in_layers", intersect=True
            )
            self.summary_report = validator.summary_report

        for layer in self.settings.feature_not_on_layers:
            validator = self.validator.create_validator(
                summary_report=self.summary_report,
                export_validation_data=self.settings.export_validation_data,
                table=layer["table"],
                table2=layer["intersection_table"],
                export_layername=layer["layername"],
                where_condition=self.build_where_statement(layer),
                message=layer["message"],
            )

            print(
                f"Running feature not on layer check between {layer['table']} and {layer['intersection_table']}"
            )

            validator.set_exports(
                export_parquet=self.settings.export_parquet,
                export_parquet_by_geometry_type=self.settings.export_parquet_by_geometry_type,
                export_gpkg=self.settings.export_gpkg,
            )
            validator.run_layer_intersections(
                rule_name="feature_not_on_layers", intersect=False
            )
            self.summary_report = validator.summary_report

        for layer in self.settings.line_not_on_feature_layers:
            validator = self.validator.create_validator(
                summary_report=self.summary_report,
                export_validation_data=self.settings.export_validation_data,
                table=layer["line_table"],
                table2=layer["intersection_table"],
                export_layername=layer["layername"],
                where_condition=self.build_where_statement(layer),
                message=layer["message"],
            )

            print(
                f"Running line not on feature layer check between {layer['line_table']} and {layer['intersection_table']}"
            )

            validator.set_exports(
                export_parquet=self.settings.export_parquet,
                export_parquet_by_geometry_type=self.settings.export_parquet_by_geometry_type,
                export_gpkg=self.settings.export_gpkg,
            )
            validator.run_layer_intersections(
                rule_name="line_not_on_feature_layers",
                intersect=False,
                buffer_lines=True,
            )
            self.summary_report = validator.summary_report

        for layer in self.settings.line_not_touches_feature_layers:
            validator = self.validator.create_validator(
                summary_report=self.summary_report,
                export_validation_data=self.settings.export_validation_data,
                table=layer["line_table"],
                table2=layer["intersection_table"],
                export_layername=layer["layername"],
                where_condition=self.build_where_statement(layer),
                message=layer["message"],
            )

            print(
                f"Running line not touches feature layer check between {layer['line_table']} and {layer['intersection_table']}"
            )

            validator.set_exports(
                export_parquet=self.settings.export_parquet,
                export_parquet_by_geometry_type=self.settings.export_parquet_by_geometry_type,
                export_gpkg=self.settings.export_gpkg,
            )
            validator.run_layer_intersections(
                rule_name="line_not_touches_feature_layers",
                intersect=False,
                buffer_lines=True,
                predicate="touches",
            )
            self.summary_report = validator.summary_report

        for layer in self.settings.feature_not_contains_layers:
            validator = self.validator.create_validator(
                summary_report=self.summary_report,
                export_validation_data=self.settings.export_validation_data,
                table=layer["table"],
                table2=layer["intersection_table"],
                export_layername=layer["layername"],
                where_condition=self.build_where_statement(layer),
                message=layer["message"],
            )

            print(
                f"Running feature not contains layer check between {layer['table']} and {layer['intersection_table']}"
            )

            validator.set_exports(
                export_parquet=self.settings.export_parquet,
                export_parquet_by_geometry_type=self.settings.export_parquet_by_geometry_type,
                export_gpkg=self.settings.export_gpkg,
            )
            validator.run_layer_intersections(
                rule_name="feature_not_contains_layers",
                intersect=False,
                buffer_lines=False,
                predicate="contains",
            )
            self.summary_report = validator.summary_report

    def run_process_self_intersections(self):
        for layer in self.settings.self_intersect_layers:
            table = layer["table"]
            export_layername = layer["layername"]
            where_condition = self.build_where_statement(layer)
            message = layer.get("message", "")

            # Will automatically create the appropriate validator
            validator = self.validator.create_validator(
                summary_report=self.summary_report,
                export_validation_data=self.settings.export_validation_data,
                table=table,
                table2=None,
                export_layername=export_layername,
                where_condition=where_condition,
                message=message,
            )
            validator.set_exports(
                export_parquet=self.settings.export_parquet,
                export_parquet_by_geometry_type=self.settings.export_parquet_by_geometry_type,
                export_gpkg=self.settings.export_gpkg,
            )
            validator.run_self_intersections(rule_name="self_intersect_layers")
            self.summary_report = validator.summary_report
