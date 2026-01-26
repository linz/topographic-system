import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine
from .base import AbstractTopologyValidator


class PostgisTopologyValidator(AbstractTopologyValidator):
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

        if not db_url.startswith("postgresql"):
            raise ValueError(
                "db_url must be a PostgreSQL connection string starting with 'postgresql'"
            )

        self.engine = create_engine(db_url)
        self.pkey = self.get_primary_key()
        self.source = "postgis"
        self.geom_column = "geom"

    def get_primary_key(self) -> str:
        """Get the primary key column name from the PostgreSQL table"""
        table = self.table.split(".")[-1]  # Get the table name without schema
        schema = self.table.split(".")[0] if "." in self.table else "public"

        # FIXME: secure against SQL injection
        sql = f"""
        SELECT
            kcu.column_name
        FROM
            information_schema.table_constraints AS tc
        JOIN
            information_schema.key_column_usage AS kcu
        ON
            tc.constraint_name = kcu.constraint_name
        WHERE
            tc.table_name = '{table}' AND tc.constraint_schema = '{schema}'
            AND tc.constraint_type = 'PRIMARY KEY';
        """

        df_table = pd.read_sql(sql, self.engine)
        if len(df_table["column_name"]) > 0:
            pk = df_table["column_name"][0]
        else:
            pk = "id"
        return pk

    def _read_data(self) -> None:
        """Read data from PostGIS database"""
        where_condition = ""
        if self.where_condition:
            where_condition = f"WHERE {self.where_condition}"
            if self.bbox:
                where_condition += f" AND {self.geom_column} && ST_MakeEnvelope({self.bbox[0]}, {self.bbox[1]}, {self.bbox[2]}, {self.bbox[3]}, 2193)"
        elif self.bbox:
            where_condition = f"WHERE {self.geom_column} && ST_MakeEnvelope({self.bbox[0]}, {self.bbox[1]}, {self.bbox[2]}, {self.bbox[3]}, 2193)"

        query = f"SELECT * FROM {self.table} {where_condition}"
        self.gdf = gpd.read_postgis(query, self.engine, geom_col=self.geom_column)

        if self.twotable:
            query2 = f"SELECT * FROM {self.table2}"
            if self.bbox:
                query2 += f" WHERE {self.geom_column} && ST_MakeEnvelope({self.bbox[0]}, {self.bbox[1]}, {self.bbox[2]}, {self.bbox[3]}, 2193)"
            self.gdf2 = gpd.read_postgis(query2, self.engine, geom_col=self.geom_column)

    def _read_data_by_rule(self, rule_is_null: bool = True, rule: str = "") -> None:
        """Read data from PostGIS database filtered by rule"""
        if self.where_condition:
            where_condition = f" AND {self.where_condition}"
        else:
            where_condition = ""

        if rule_is_null:
            column_name = rule
            where = f"WHERE {column_name} IS NULL {where_condition}"
        else:
            where = f"WHERE {rule} {where_condition}"

        if self.bbox:
            where += f" AND {self.geom_column} && ST_MakeEnvelope({self.bbox[0]}, {self.bbox[1]}, {self.bbox[2]}, {self.bbox[3]}, 2193)"

        query = f"SELECT {self.pkey}, {self.geom_column} FROM {self.table} {where}"
        self.gdf = gpd.read_postgis(query, self.engine, geom_col=self.geom_column)
