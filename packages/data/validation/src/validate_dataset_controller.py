import time
from topology_validator_factory import TopologyValidatorFactory
from topology_validator_tools import TopoValidatorSettings, TopoValidatorTools

class ValidateDatasetController:
    def __init__(self, settings: TopoValidatorSettings):
        self.settings = settings
        
    def build_where_statement(self, active_dict):
        #CHANGE TO SETTINGS
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
        self.settings.output_dir = folders.prep_output_folder(output_dir=self.settings.output_dir, use_date=self.settings.use_date_folder)
        self.validator = TopologyValidatorFactory(self.settings)
        all_processes_start_time = time.time()
        print("Starting validation run...")

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
        print(f"All processes completed. Total time taken: {seconds:.2f} seconds ({minutes:.2f} minutes)")

    def run_process_queries(self):

        for null_check in self.settings.null_columns:
            table = null_check["table"]
            export_layername = null_check["table"]
            where_condition = self.build_where_statement(null_check)
            message = null_check.get("message", "")

            validator = self.validator.create_validator(table=table, export_layername=export_layername, 
                                                       where_condition=where_condition, message=message)
            validator.set_exports(export_parquet=self.settings.export_parquet, 
                                    export_parquet_by_geometry_type=self.settings.export_parquet_by_geometry_type, 
                                    export_gpkg=self.settings.export_gpkg)
            validator.run_null_column_checks(column_name=null_check["column"])

        for query_rule in self.settings.query_rules:
            table = query_rule["table"]
            export_layername = query_rule["table"]
            where_condition = self.build_where_statement(query_rule)
            message = query_rule.get("message", "")

            validator = self.validator.create_validator(table=table, export_layername=export_layername, 
                                                        where_condition=where_condition, message=message)
            
            validator.set_exports(export_parquet=self.settings.export_parquet, 
                                    export_parquet_by_geometry_type=self.settings.export_parquet_by_geometry_type, 
                                    export_gpkg=self.settings.export_gpkg)
            
            validator.run_query_rule_checks(rule=query_rule["rule"], column_name=query_rule["column"])

    def run_process_features_on_layer(self):
        for layer in self.settings.feature_in_layers:
            validator = self.validator.create_validator(
                table=layer["table"],
                table2=layer["intersection_table"],
                export_layername=layer["layername"],
                where_condition=self.build_where_statement(layer),
                message=layer["message"],
            )
            validator.set_exports(export_parquet=self.settings.export_parquet, 
                        export_parquet_by_geometry_type=self.settings.export_parquet_by_geometry_type, 
                        export_gpkg=self.settings.export_gpkg)
            validator.run_layer_intersections()

        for layer in self.settings.feature_not_on_layers:
            validator = self.validator.create_validator(
                table=layer["table"],
                table2=layer["intersection_table"],
                export_layername=layer["layername"],
                where_condition=self.build_where_statement(layer),
                message=layer["message"]
            )
            validator.set_exports(export_parquet=self.settings.export_parquet, 
                                  export_parquet_by_geometry_type=self.settings.export_parquet_by_geometry_type, 
                                  export_gpkg=self.settings.export_gpkg)
            validator.run_layer_intersections(intersect=False)

        for layer in self.settings.line_not_on_feature_layers:
            validator = self.validator.create_validator(
                table=layer["line_table"],
                table2=layer["intersection_table"],
                export_layername=layer["layername"],
                where_condition=self.build_where_statement(layer),
                message=layer["message"]
            )
            validator.set_exports(export_parquet=self.settings.export_parquet, 
                                  export_parquet_by_geometry_type=self.settings.export_parquet_by_geometry_type, 
                                  export_gpkg=self.settings.export_gpkg)
            validator.run_layer_intersections(intersect=False, buffer_lines=True)

        for layer in self.settings.line_not_touches_feature_layers:
            validator = self.validator.create_validator(
                table=layer["line_table"],
                table2=layer["intersection_table"],
                export_layername=layer["layername"],
                where_condition=self.build_where_statement(layer),
                message=layer["message"]
            )
            validator.set_exports(export_parquet=self.settings.export_parquet, 
                                  export_parquet_by_geometry_type=self.settings.export_parquet_by_geometry_type, 
                                  export_gpkg=self.settings.export_gpkg)
            validator.run_layer_intersections(intersect=False, buffer_lines=True, predicate='touches')


        for layer in self.settings.feature_not_contains_layers:
            validator = self.validator.create_validator(
                table=layer["table"],
                table2=layer["intersection_table"],
                export_layername=layer["layername"],
                where_condition=self.build_where_statement(layer),
                message=layer["message"]
            )
            validator.set_exports(export_parquet=self.settings.export_parquet, 
                                  export_parquet_by_geometry_type=self.settings.export_parquet_by_geometry_type, 
                                  export_gpkg=self.settings.export_gpkg)
            validator.run_layer_intersections(intersect=False, buffer_lines=False, predicate='contains')

    def run_process_self_intersections(self):
        for layer in self.settings.self_intersect_layers:
            table = layer["table"]
            export_layername = layer["layername"]
            where_condition = self.build_where_statement(layer)
            message = layer.get("message", "")
            
            # Will automatically create the appropriate validator
            validator = self.validator.create_validator(table, None, export_layername, where_condition=where_condition, message=message)
            validator.set_exports(export_parquet=self.settings.export_parquet, 
                                    export_parquet_by_geometry_type=self.settings.export_parquet_by_geometry_type, 
                                    export_gpkg=self.settings.export_gpkg)
            validator.run_self_intersections()


