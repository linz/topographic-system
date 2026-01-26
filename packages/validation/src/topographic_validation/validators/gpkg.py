import geopandas as gpd
from .base import AbstractTopologyValidator


class GpkgTopologyValidator(AbstractTopologyValidator):
    def __init__(
        self,
        summary_report: dict[str, bool | str],
        export_validation_data: bool,
        db_url: str,
        table: str,
        export_layername: str,
        table2: str | None = None,
        where_condition: str | None = None,
        bbox: tuple[float, float, float, float] | None = None,
        message: str | None = None,
        output_dir: str = "./topoedit/validation-data",
        area_crs: int = 2193,
    ) -> None:
        if message is None:
            message = "validation error"
        super().__init__(
            summary_report,
            export_validation_data,
            db_url,
            table,
            export_layername,
            table2,
            where_condition,
            bbox,
            message,
            output_dir,
            area_crs,
        )
        if not db_url.endswith(".gpkg"):
            raise ValueError("db_url must be a GeoPackage file path ending with .gpkg")

        self.pkey = "topo_id"
        self.source = "gpkg"
        self.geom_column = "geometry"

    def _read_data(self) -> None:
        """Read data from GeoPackage file"""
        self.gdf = gpd.read_file(
            self.db_url,
            layer=self.table,
            where=self.where_condition,
            bbox=self.bbox,
        )
        if self.table2 != self.table:
            self.gdf2 = gpd.read_file(self.db_url, layer=self.table2, bbox=self.bbox)

    def _read_data_by_rule(self, rule_is_null: bool = True, rule: str = "") -> None:
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

        self.gdf = gpd.read_file(
            self.db_url,
            layer=self.table,
            columns=[self.pkey, self.geom_column],
            where=where,
            bbox=self.bbox,
        )
