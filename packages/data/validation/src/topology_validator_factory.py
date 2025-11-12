from topology_validator_tools import TopoValidatorSettings
from gpkg_topology_validator import GpkgTopologyValidator
from parquet_topology_validator import ParquetTopologyValidator
from postgis_topology_validator import PostgisTopologyValidator

"""Factory class to create the appropriate TopologyValidator based on db_path"""
class TopologyValidatorFactory():
    def __init__(self, settings: TopoValidatorSettings):
        self.settings = settings

    def create_validator(self, table, table2=None, export_layername=None, where_condition=None, message=None):
        """
        Create and return the appropriate TopologyValidator instance based on db_path
        
        Args: settings: TopoValidatorSettings instance containing configuration
            db_path: Database URL or file path
            table: Table/layer name
            export_layername: Name for exported validation layers
            table2: Second table/layer name for two-table operations (optional)
            where_condition: SQL WHERE condition (optional)
            bbox: Bounding box for spatial filtering (optional)
            message: Validation error message (optional)
            output_dir: Output directory for validation results (optional)
            area_crs: CRS for area calculations (optional)
            
        Returns:
            Appropriate TopologyValidator instance
            
        Raises:
            ValueError: If db_path format is not recognized
        """
        if self.settings.db_path.startswith('postgresql'):
            return PostgisTopologyValidator(
                self.settings.db_path, table, export_layername, table2,
                where_condition, self.settings.bbox, message, self.settings.output_dir, self.settings.area_crs
            )
        elif self.settings.db_path.endswith('.gpkg'):
            return GpkgTopologyValidator(
                self.settings.db_path, table, export_layername, table2,
                where_condition, self.settings.bbox, message, self.settings.output_dir, self.settings.area_crs
            )
        elif self.settings.db_path.endswith('.parquet') or 'parquet' in self.settings.db_path:
            return ParquetTopologyValidator(
                self.settings.db_path, table, export_layername, table2,
                where_condition, self.settings.bbox, message, self.settings.output_dir, self.settings.area_crs
            )
        else:
            raise ValueError(
                "db_path must be a PostgreSQL connection string, "
                "a GeoPackage file path (.gpkg), or a Parquet file path (.parquet)"
            )
