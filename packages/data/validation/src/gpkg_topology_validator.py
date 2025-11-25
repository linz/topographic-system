import geopandas as gpd
from abstract_topology_validator import AbstractTopologyValidator


class GpkgTopologyValidator(AbstractTopologyValidator):
    def __init__(self, summary_report, export_validation_data, db_url, table, export_layername, table2=None,  
                 where_condition=None, bbox=None,
                 message="validation error", 
                 output_dir=r"c:\data\topoedit\validation-data", 
                 area_crs=2193):
        
        super().__init__(summary_report, export_validation_data, db_url, table, export_layername, table2, 
                         where_condition, bbox, message, output_dir, area_crs)
        
        if not db_url.endswith('.gpkg'):
            raise ValueError("db_url must be a GeoPackage file path ending with .gpkg")
        
        self.pkey = 'topo_id'
        self.source = 'gpkg'
        self.geom_column = 'geometry'

    def _read_data(self):
        """Read data from GeoPackage file"""
        if self.where_condition and self.bbox:
            self.gdf = gpd.read_file(self.db_url, layer=self.table, where=self.where_condition, bbox=self.bbox)
        elif self.where_condition:
            self.gdf = gpd.read_file(self.db_url, layer=self.table, where=self.where_condition)
        elif self.bbox:
            self.gdf = gpd.read_file(self.db_url, layer=self.table, bbox=self.bbox)
        else:
            self.gdf = gpd.read_file(self.db_url, layer=self.table)
        
        if self.mode == 'twotable':
            if self.bbox:
                self.gdf2 = gpd.read_file(self.db_url, layer=self.table2, bbox=self.bbox)
            else:
                self.gdf2 = gpd.read_file(self.db_url, layer=self.table2)

    def _read_data_by_rule(self, rule_is_null=True, rule=''):
        """Read data from GeoPackage file filtered by rule"""
        if self.where_condition:
            where_condition = f" AND {self.where_condition}"
        else:
            where_condition = ""

        if rule_is_null:
            column_name = rule
            where = f"{column_name} IS NULL {where_condition}"
        else:
            where = f"{rule} {where_condition}"

        if self.bbox:
            self.gdf = gpd.read_file(self.db_url, layer=self.table, 
                                     columns=[self.pkey, self.geom_column], 
                                     where=where, bbox=self.bbox)
        else:
            self.gdf = gpd.read_file(self.db_url, layer=self.table, 
                                    columns=[self.pkey, self.geom_column], 
                                    where=where)