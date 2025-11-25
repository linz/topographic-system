from topology_validator_tools import TopoValidatorSettings
from validate_dataset_controller import ValidateDatasetController
if __name__ == "__main__":

    mode = 'generic'
    settings = TopoValidatorSettings()

    # set data source and read configuration - postgres or gkpg and parquet
    if mode == 'postgis': 
        settings.validation_config_file = './validation/src/validation_postgis_config.json'
        settings.db_path = "postgresql://postgres:landinformation@localhost:5432/topo"
    else:
        # set data source and read configuration - gkpg and parquet
        settings.validation_config_file = './packages/data/validation/src/validation_generic_config.json'
        settings.db_path = r"C:\Data\topoedit\topographic-data\topographic-data.gpkg"
        settings.db_path = r"C:\Data\toposource\topographic-data\topographic-data.gpkg"
        #settings.db_path = r"C:\Data\temp\2025-02-05\files.parquet"

    settings.output_dir = r"C:\Data\topoedit\validation-data"
    settings.output_dir = r"C:\temp\validation-data"
    settings.export_validation_data = True
    settings.area_crs = 2193
    settings.export_parquet = False
    settings.export_parquet_by_geometry_type = False
    settings.export_gpkg = True
    settings.use_date_folder = True
    settings.process_queries = True
    settings.process_features_on_layer = True
    settings.process_self_intersections = True

    #settings.bbox = (174.81, -41.31, 174.82, -41.30)
    settings.bbox = (174.711, -41.349, 175.04, -41.17)
    #settings.date = "today"
    ## settings.date =  "2025-10-01"
    #settings.weeks = 1

    ValidateDatasetController(settings).run_validation()

