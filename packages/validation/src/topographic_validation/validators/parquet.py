import os
import geopandas as gpd
from topographic_validation.validators.base import AbstractTopologyValidator


class ParquetTopologyValidator(AbstractTopologyValidator):
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

        if not db_url.endswith(".parquet"):
            raise ValueError("db_url must be a Parquet file path ending with .parquet")

        # Remove 'files.parquet' from the end if present
        self.db_url = db_url.replace("files.parquet", "")
        self.pkey = "topo_id"
        self.source = "parquet"
        self.geom_column = "geom"

    def _read_data(self) -> None:
        """Read data from Parquet files"""
        file = os.path.join(self.db_url, f"{self.table}.parquet")

        gdf = gpd.read_parquet(file, bbox=self.bbox)
        if self.where_condition:
            # Note: read_parquet does not support where directly
            where = (
                self.where_condition.replace("=", "==")
                .replace("AND", "&")
                .replace("OR", "|")
            )
            gdf = gdf.query(where)
        self.gdf = gdf

        if self.table2 != self.table:
            file = os.path.join(self.db_url, f"{self.table2}.parquet")
            self.gdf2 = gpd.read_parquet(file, bbox=self.bbox)

    def _read_data_by_rule(self, rule_is_null: bool = True, rule: str = "") -> None:
        """Read data from Parquet file filtered by rule"""
        file = os.path.join(self.db_url, f"{self.table}.parquet")
        gdf = gpd.read_parquet(file, bbox=self.bbox)

        if rule_is_null:
            column_name = rule
            null_gdf = gdf[gdf[column_name].isna()]
        else:
            null_gdf = gdf

        if self.where_condition:
            where = self.where_condition
            # Convert SQL-style conditions to pandas query format
            if "OR" in where or "AND" in where:
                where = where.replace("(", "").replace(")", "")
            where = where.replace("=", "==").replace("AND", "&").replace("OR", "|")
            where = where.replace("IN", "in").replace("NOT", "not").replace("IS", "is")

            if "is not null" in where:
                where = "~" + where.replace("is not null", ".isnull()")
            elif "is null" in where:
                where = where.replace("is null", ".isnull()")

            if " in " in where:
                where = where.replace("(", "[").replace(")", "]")

            null_gdf = null_gdf.query(where)

        self.gdf = null_gdf[[self.pkey, self.geom_column]]
