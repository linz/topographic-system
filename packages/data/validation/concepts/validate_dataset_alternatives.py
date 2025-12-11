import os
import geopandas as gpd
import pandas as pd
from shapely import Point
from sqlalchemy import create_engine
import sqlite3
import shutil
import datetime


class TopologyValidator:
    def __init__(
        self,
        db_url,
        table,
        export_layername,
        table2=None,
        where_condition=None,
        bbox=None,
        message="validation error",
        output_dir=r"c:\data\topoedit\validation-data",
        area_crs=2193,
    ):
        self.db_url = db_url
        self.table = table
        if table2 is None:
            self.table2 = table
            self.mode = "onetable"
        else:
            self.table2 = table2
            self.mode = "twotable"
        self.layername = export_layername
        self.where_condition = where_condition
        self.output_dir = output_dir
        self.area_crs = area_crs
        self.message = message
        self.bbox = bbox
        if db_url.startswith("postgresql"):
            self.engine = create_engine(db_url)
            self.pkey = self.get_primary_key()
            self.source = "postgis"
            self.geom_column = "geom"
        else:
            spatialite_path = (
                os.environ.get("USERPROFILE") + r"\AppData\Local\miniconda3\Library\bin"
            )
            self.set_mod_spatialite(spatialite_path)
            self.engine = sqlite3.connect(db_url)
            os.chdir(
                self.spatialite_path
            )  # Temporarily switch working directory to Spatialite extensions path
            self.engine.enable_load_extension(True)
            self.engine.load_extension("mod_spatialite.dll")
            os.chdir(self.current_dir)  # Switch back to original working directory

            self.pkey = "topo_id"
            self.source = "gpkg"
            self.geom_column = "geometry"
            self.gdf = None
            self.sindex = None
            self.gdf2 = None

        self.set_exports(True, False, True)

    def set_exports(
        self,
        export_parquet=True,
        export_parquet_by_geometry_type=True,
        export_gpkg=True,
    ):
        self.export_parquet = export_parquet
        self.export_parquet_by_geometry_type = export_parquet_by_geometry_type
        self.export_gpkg = export_gpkg

    def set_mod_spatialite(self, path):
        self.spatialite_path = path
        self.current_dir = os.getcwd()

    def read_data_intersect(self):
        if self.where_condition:
            if self.source == "gpkg":
                self.gdf = gpd.read_file(
                    self.db_url, layer=self.table, where=self.where_condition
                )
                if self.mode == "twotable":
                    self.gdf2 = gpd.read_file(self.db_url, layer=self.table2)
            else:
                query = f"""
                    SELECT a.*
                    FROM {self.table} a
                    JOIN {self.table2} b
                    ON a.{self.geom_column} && b.{self.geom_column} 
                    WHERE ST_Intersects(a.{self.geom_column}, b.{self.geom_column}) 
                    AND a.{self.where_condition} 
                    AND a.topo_id != b.topo_id;                    
                    """
                self.gdf = gpd.read_postgis(
                    query, self.engine, geom_col=self.geom_column
                )
                if self.mode == "twotable":
                    self.gdf2 = gpd.read_postgis(
                        f"SELECT * FROM {self.table2}",
                        self.engine,
                        geom_col=self.geom_column,
                    )
        elif self.bbox:
            if self.source == "gpkg":
                self.gdf = gpd.read_file(self.db_url, layer=self.table, bbox=self.bbox)
                if self.mode == "twotable":
                    self.gdf2 = gpd.read_file(
                        self.db_url, layer=self.table2, bbox=self.bbox
                    )
            else:
                query = f"""
                   SELECT  a.*
                    FROM {self.table} a
                    JOIN {self.table2} b
                    ON a.{self.geom_column} && b.{self.geom_column} 
                    WHERE ST_Intersects(a.{self.geom_column}, b.{self.geom_column}) 
                    AND ST_Intersects(a.{self.geom_column}, ST_MakeEnvelope({self.bbox[0]}, {self.bbox[1]}, {self.bbox[2]}, {self.bbox[3]}, 2193))
                    AND a.topo_id != b.topo_id;                    
                    """
                self.gdf = gpd.read_postgis(
                    query, self.engine, geom_col=self.geom_column
                )
                if self.mode == "twotable":
                    self.gdf2 = gpd.read_postgis(
                        f"SELECT * FROM {self.table2}",
                        self.engine,
                        geom_col=self.geom_column,
                    )
        else:
            if self.source == "gpkg":
                self.gdf = gpd.read_file(self.db_url, layer=self.table)
                if self.mode == "twotable":
                    self.gdf2 = gpd.read_file(self.db_url, layer=self.table2)
            else:
                query = f"""
                   SELECT  a.*
                    FROM {self.table} a
                    JOIN {self.table2} b
                    ON a.{self.geom_column} && b.{self.geom_column} 
                    WHERE ST_Intersects(a.{self.geom_column}, b.{self.geom_column}) 
                    AND a.topo_id != b.topo_id;                    
                    """
                self.gdf = gpd.read_postgis(
                    f"SELECT * FROM {self.table}",
                    self.engine,
                    geom_col=self.geom_column,
                )
                if self.mode == "twotable":
                    self.gdf2 = gpd.read_postgis(
                        f"SELECT * FROM {self.table2}",
                        self.engine,
                        geom_col=self.geom_column,
                    )
        if self.mode == "onetable":
            self.sindex = self.gdf.sindex
        else:
            self.sindex = self.gdf2.sindex

    def read_data_where_column_null(self, column_name):
        if self.where_condition:
            where_condition = f" AND {self.where_condition}"
        else:
            where_condition = ""

        if self.source == "gpkg":
            where = f"{column_name} IS NULL {where_condition}"
            self.gdf = gpd.read_file(
                self.db_url,
                layer=self.table,
                columns=[self.pkey, self.geom_column],
                where=where,
            )
        else:
            query = f"SELECT {self.pkey}, {self.geom_column} FROM {self.table} WHERE {column_name} IS NULL {where_condition}"
            self.gdf = gpd.read_postgis(query, self.engine, geom_col=self.geom_column)
        self.sindex = self.gdf.sindex

    def read_data_where_rule(self, rule):
        if self.where_condition:
            where_condition = f" AND {self.where_condition}"
        else:
            where_condition = ""

        if self.source == "gpkg":
            where = f"{rule} {where_condition}"
            self.gdf = gpd.read_file(
                self.db_url,
                layer=self.table,
                columns=[self.pkey, self.geom_column],
                where=where,
            )
        else:
            query = f"SELECT {self.pkey}, {self.geom_column} FROM {self.table} WHERE {rule} {where_condition}"
            self.gdf = gpd.read_postgis(query, self.engine, geom_col=self.geom_column)
        self.sindex = self.gdf.sindex

    def get_names(self, gdf, idx1, idx2):
        if hasattr(gdf, "name"):
            return (gdf.name.iloc[idx1], gdf.name.iloc[idx2])
        else:
            return ("noname", "noname")

    def get_keys(self, gdf, idx1, idx2):
        return (gdf[self.pkey].iloc[idx1], gdf[self.pkey].iloc[idx2])

    def get_primary_key(self):
        table = self.table.split(".")[-1]  # Get the table name without schema
        schema = self.table.split(".")[0]
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

    def handle_geometry_collection(self, intersection_geom, original_names):
        geoms = []
        for geom in intersection_geom.geoms:
            entry = {
                "geometry": self.geom_column,
                "pair_names": f"{original_names[0]}-{original_names[1]}",
            }
            geoms.append((geom.geom_type, entry))
        return geoms

    def find_intersections_features_in_layer(self):
        starttime = datetime.datetime.now()

        # print(self.gdf.sindex.valid_query_predicates)
        # print(self.gdf2.sindex.valid_query_predicates)

        intersecting_features = gpd.sjoin(self.gdf, self.gdf2, how="inner")
        intersecting_features.columns = [
            col.replace("_left", "") for col in intersecting_features.columns
        ]
        columns = [self.pkey, self.geom_column]
        if self.pkey != "topo_id":
            columns.append("topo_id")
        if "name" in intersecting_features.columns:
            columns.append("name")

        intersecting_features = intersecting_features[columns]

        endtime = datetime.datetime.now()
        print("Time taken for spatial index query:", endtime - starttime)

        return intersecting_features

    def find_not_intersections_features_in_layer(self, buffer_lines=True):
        if buffer_lines and self.gdf2.geom_type.iloc[0] in [
            "LineString",
            "MultiLineString",
        ]:
            buffer_lines = 0.000001
            self.gdf2[self.geom_column] = self.gdf2.geometry.buffer(buffer_lines)

        intersecting_features = gpd.sjoin(self.gdf, self.gdf2, how="left")
        non_intersecting_features = intersecting_features[
            intersecting_features["index_right"].isna()
        ]
        non_intersecting_features.columns = [
            col.replace("_left", "") for col in non_intersecting_features.columns
        ]
        columns = [self.pkey, self.geom_column]
        if self.pkey != "topo_id":
            columns.append("topo_id")
        if "name" in intersecting_features.columns:
            columns.append("name")

        non_intersecting_features = non_intersecting_features[columns]

        return non_intersecting_features

    def find_not_intersections_line_on_geom(self, buffer_lines=True):
        if buffer_lines and self.gdf2.geom_type.iloc[0] in [
            "LineString",
            "MultiLineString",
        ]:
            buffer_lines = 0.000001
            self.gdf2[self.geom_column] = self.gdf2.geometry.buffer(buffer_lines)

        intersecting_features = gpd.sjoin(self.gdf, self.gdf2, how="left")
        non_intersecting_features = intersecting_features[
            intersecting_features["index_right"].isna()
        ]
        non_intersecting_features.columns = [
            col.replace("_left", "") for col in non_intersecting_features.columns
        ]
        columns = [self.pkey, self.geom_column]
        if self.pkey != "topo_id":
            columns.append("topo_id")
        if "name" in intersecting_features.columns:
            columns.append("name")

        non_intersecting_features = non_intersecting_features[columns]

        return non_intersecting_features

    def find_not_intersections_touches_on_geom(self, buffer_lines=True):
        if buffer_lines and self.gdf2.geom_type.iloc[0] in [
            "LineString",
            "MultiLineString",
        ]:
            buffer_lines = 0.000001
            self.gdf2[self.geom_column] = self.gdf2.geometry.buffer(buffer_lines)

        # print(self.gdf.sindex.valid_query_predicates)

        intersecting_features = gpd.sjoin(
            self.gdf, self.gdf2, how="left", predicate="touches"
        )
        non_intersecting_features = intersecting_features[
            intersecting_features["index_right"].isna()
        ]
        non_intersecting_features.columns = [
            col.replace("_left", "") for col in non_intersecting_features.columns
        ]
        columns = [self.pkey, self.geom_column]
        if self.pkey != "topo_id":
            columns.append("topo_id")
        if "name" in intersecting_features.columns:
            columns.append("name")

        non_intersecting_features = non_intersecting_features[columns]

        return non_intersecting_features

    def find_not_contains_geom(self):
        intersecting_features = gpd.sjoin(
            self.gdf, self.gdf2, how="left", predicate="contains"
        )
        non_intersecting_features = intersecting_features[
            intersecting_features["index_right"].isna()
        ]
        non_intersecting_features.columns = [
            col.replace("_left", "") for col in non_intersecting_features.columns
        ]
        columns = [self.pkey, self.geom_column]
        if self.pkey != "topo_id":
            columns.append("topo_id")
        if "name" in intersecting_features.columns:
            columns.append("name")

        non_intersecting_features = non_intersecting_features[columns]

        return non_intersecting_features

    def get_first_last(geom):
        if geom.geom_type == "LineString":
            return Point(geom.coords[0]), Point(geom.coords[-1])
        elif geom.geom_type == "MultiLineString":
            first_line = geom.geoms[0]
            last_line = geom.geoms[-1]
            return Point(first_line.coords[0]), Point(last_line.coords[-1])
        else:
            return None, None

    def find_self_intersecting(self):
        candidate_pairs = set()
        starttime = datetime.datetime.now()

        for idx1, row in self.gdf.iterrows():
            geom = row.get("geom", None) or row.get("geometry", None)

            for idx2 in self.sindex.query(geom):
                if idx1 < idx2:
                    candidate_pairs.add((idx1, idx2))

        endtime = datetime.datetime.now()
        print("Time taken for spatial index query:", endtime - starttime)

        intersection_geometries = []
        intersection_geometries_point = []
        intersection_geometries_line = []
        intersection_geometries_multipolygon = []
        geomtypes = []
        for idx1, idx2 in candidate_pairs:
            geom1 = self.gdf.geometry.iloc[idx1]
            if self.mode == "onetable":
                geom2 = self.gdf.geometry.iloc[idx2]
            else:
                geom2 = self.gdf2.geometry.iloc[idx2]
            if geom1.intersects(geom2):
                intersection_geom = geom1.intersection(geom2)
                geomtypes.append(intersection_geom.geom_type)
                if not intersection_geom.is_empty:
                    original_names = self.get_names(self.gdf, idx1, idx2)
                    original_keys = self.get_keys(self.gdf, idx1, idx2)
                    if intersection_geom.geom_type in ["Point", "MultiPoint"]:
                        intersection_geometries_point.append(
                            {
                                "geometry": intersection_geom,
                                "pair_names": f"{original_names[0]}-{original_names[1]}",
                                "pair_keys": f"{original_keys[0]}-{original_keys[1]}",
                            }
                        )
                    elif intersection_geom.geom_type in [
                        "LineString",
                        "MultiLineString",
                    ]:
                        intersection_geometries_line.append(
                            {
                                "geometry": intersection_geom,
                                "pair_names": f"{original_names[0]}-{original_names[1]}",
                                "pair_keys": f"{original_keys[0]}-{original_keys[1]}",
                            }
                        )
                    elif intersection_geom.geom_type == "GeometryCollection":
                        for geom_type, entry in self.handle_geometry_collection(
                            intersection_geom, original_names
                        ):
                            if geom_type in ["Point", "MultiPoint"]:
                                intersection_geometries_point.append(entry)
                            elif geom_type in ["LineString", "MultiLineString"]:
                                intersection_geometries_line.append(entry)
                            else:
                                intersection_geometries.append(entry)
                    elif intersection_geom.geom_type == "MultiPolygon":
                        for geom in intersection_geom.geoms:
                            entry = {
                                "geometry": geom,
                                "pair_names": f"{original_names[0]}-{original_names[1]}",
                                "pair_keys": f"{original_keys[0]}-{original_keys[1]}",
                            }
                            intersection_geometries_multipolygon.append(entry)
                    else:
                        intersection_geometries.append(
                            {
                                "geometry": intersection_geom,
                                "pair_names": f"{original_names[0]}-{original_names[1]}",
                                "pair_keys": f"{original_keys[0]}-{original_keys[1]}",
                            }
                        )
        return (
            intersection_geometries,
            intersection_geometries_point,
            intersection_geometries_line,
            intersection_geometries_multipolygon,
            geomtypes,
        )

    def get_valid_geometries(self, geometries):
        all_intersection_geometries_valid = []
        for item in geometries:
            geom = item["geometry"]
            if isinstance(geom, str):
                # print(item)
                continue
            if geom.is_valid:
                all_intersection_geometries_valid.append(item)
                # print(type(geom))
            else:
                continue
                # print(item)
        return all_intersection_geometries_valid

    def save_gdf(self, gdf):
        if gdf.empty:
            print("No points in polygons found.")
            return
        gdf["warning"] = self.message
        gdf["status"] = "open"
        gdf["val_date"] = datetime.datetime.now().strftime("%Y-%m-%d")
        if self.export_parquet:
            gdf.to_parquet(
                rf"{self.output_dir}\{self.layername}.parquet",
                engine="pyarrow",
                compression="zstd",
                write_covering_bbox=True,
                row_group_size=50000,
            )
        if self.export_gpkg:
            gdf.to_file(
                rf"{self.output_dir}\topology_checks.gpkg",
                layer=f"{self.layername}",
                driver="GPKG",
                append=True,
            )

    def save_gdf_outputs(self, column_name, check_name="null"):
        if self.gdf.empty:
            print(f"No records with {check_name} {column_name} found.")
            return
        self.gdf["warning"] = self.message
        self.gdf["status"] = "open"
        if self.export_parquet:
            self.gdf.to_parquet(
                rf"{self.output_dir}\{self.layername}_{check_name}_{column_name}.parquet",
                engine="pyarrow",
                compression="zstd",
                write_covering_bbox=True,
                row_group_size=50000,
            )
        if self.export_gpkg:
            self.gdf.to_file(
                rf"{self.output_dir}\topology_{check_name}_checks.gpkg",
                layer=f"{self.layername}_{check_name}_{column_name}",
                driver="GPKG",
                append=True,
            )

    def save_intersection_outputs(
        self,
        intersection_geometries,
        intersection_geometries_point,
        intersection_geometries_line,
        intersection_geometries_multipolygon,
    ):
        # Combine all intersection geometries into a single list
        intersection_geometries = self.get_valid_geometries(intersection_geometries)
        intersection_geometries_point = self.get_valid_geometries(
            intersection_geometries_point
        )
        intersection_geometries_line = self.get_valid_geometries(
            intersection_geometries_line
        )
        intersection_geometries_multipolygon = self.get_valid_geometries(
            intersection_geometries_multipolygon
        )

        validation_date = datetime.datetime.now().strftime("%Y-%m-%d")
        all_intersection_geometries = []

        if intersection_geometries:
            all_intersection_geometries += intersection_geometries
        if intersection_geometries_point:
            all_intersection_geometries += intersection_geometries_point
        if intersection_geometries_line:
            all_intersection_geometries += intersection_geometries_line
        if intersection_geometries_multipolygon:
            all_intersection_geometries += intersection_geometries_multipolygon
        if len(all_intersection_geometries) == 0:
            print("No topology errors found")
            return

        if self.export_parquet:
            intersections_gdf = gpd.GeoDataFrame(
                all_intersection_geometries, geometry=self.geom_column, crs=self.gdf.crs
            )
            intersections_gdf["warning"] = self.message
            intersections_gdf["status"] = "open"
            intersections_gdf["val_date"] = validation_date
            intersections_gdf.to_parquet(
                rf"{self.output_dir}\{self.layername}_topology_checks.parquet",
                engine="pyarrow",
                compression="zstd",
                write_covering_bbox=True,
                row_group_size=50000,
            )

        if self.export_gpkg or self.export_parquet_by_geometry_type:
            if len(intersection_geometries) > 0:
                intersections_gdf = gpd.GeoDataFrame(
                    intersection_geometries, geometry=self.geom_column, crs=self.gdf.crs
                )
                intersections_gdf = intersections_gdf.to_crs(epsg=self.area_crs)
                intersections_gdf["Area"] = intersections_gdf.geometry.area
                intersections_gdf["warning"] = self.message
                intersections_gdf["status"] = "open"
                intersections_gdf["val_date"] = validation_date
                if self.export_gpkg:
                    intersections_gdf.to_file(
                        rf"{self.output_dir}\topology_checks.gpkg",
                        layer=f"{self.layername}_errors_areas",
                        driver="GPKG",
                    )
                if self.export_parquet_by_geometry_type:
                    intersections_gdf.to_parquet(
                        rf"{self.output_dir}\{self.layername}_topology_checks_poly.parquet",
                        engine="pyarrow",
                        compression="zstd",
                        write_covering_bbox=True,
                        row_group_size=50000,
                    )

            if len(intersection_geometries_point) > 0:
                intersections_gdf = gpd.GeoDataFrame(
                    intersection_geometries_point, crs=self.gdf.crs
                )
                intersections_gdf["warning"] = self.message
                intersections_gdf["status"] = "open"
                intersections_gdf["val_date"] = validation_date

                if self.export_gpkg:
                    intersections_gdf.to_file(
                        rf"{self.output_dir}\topology_checks.gpkg",
                        layer=f"{self.layername}_errors_points",
                        driver="GPKG",
                    )
                if self.export_parquet_by_geometry_type:
                    intersections_gdf.to_parquet(
                        rf"{self.output_dir}\{self.layername}_topology_checks_point.parquet",
                        engine="pyarrow",
                        compression="zstd",
                        write_covering_bbox=True,
                        row_group_size=50000,
                    )

            if len(intersection_geometries_line) > 0:
                intersections_gdf = gpd.GeoDataFrame(
                    intersection_geometries_line, crs=self.gdf.crs
                )
                intersections_gdf["warning"] = self.message
                intersections_gdf["status"] = "open"
                intersections_gdf["val_date"] = validation_date
                if self.export_gpkg:
                    intersections_gdf.to_file(
                        rf"{self.output_dir}\topology_checks.gpkg",
                        layer=f"{self.layername}_errors_lines",
                        driver="GPKG",
                    )
                if self.export_parquet_by_geometry_type:
                    intersections_gdf.to_parquet(
                        rf"{self.output_dir}\{self.layername}_topology_checks_line.parquet",
                        engine="pyarrow",
                        compression="zstd",
                        write_covering_bbox=True,
                        row_group_size=50000,
                    )

            # Save multipolygon intersections if any
            if len(intersection_geometries_multipolygon) > 0:
                intersections_gdf = gpd.GeoDataFrame(
                    intersection_geometries_multipolygon, crs=self.gdf.crs
                )
                intersections_gdf["warning"] = self.message
                intersections_gdf["status"] = "open"
                intersections_gdf["val_date"] = validation_date
                if self.export_gpkg:
                    intersections_gdf.to_file(
                        rf"{self.output_dir}\topology_checks.gpkg",
                        layer=f"{self.layername}_errors_multipolygon",
                        driver="GPKG",
                    )
                if self.export_parquet_by_geometry_type:
                    intersections_gdf.to_parquet(
                        rf"{self.output_dir}\{self.layername}_topology_checks_multipolygon.parquet",
                        engine="pyarrow",
                        compression="zstd",
                        write_covering_bbox=True,
                        row_group_size=50000,
                    )

    def run_intersections(self):
        starttime = datetime.datetime.now()
        self.read_data_intersect()
        endtime = datetime.datetime.now()
        print("Time taken for read_dataset:", endtime - starttime)

        # starttime = datetime.datetime.now()
        # self.read_data_intersect_trial()
        # endtime = datetime.datetime.now()
        # print("Time taken for read_data_intersect_trial:", endtime - starttime)

        (
            intersection_geometries,
            intersection_geometries_point,
            intersection_geometries_line,
            intersection_geometries_multipolygon,
            geomtypes,
        ) = self.find_self_intersecting()
        print("Unique intersection geometry types:", list(set(geomtypes)))
        self.save_intersection_outputs(
            intersection_geometries,
            intersection_geometries_point,
            intersection_geometries_line,
            intersection_geometries_multipolygon,
        )

    def run_features_intersect_layer(self, intersect=True, buffer_lines=True):
        self.read_data_intersect()

        if intersect:
            gdf = self.find_intersections_features_in_layer()
        else:
            gdf = self.find_not_intersections_features_in_layer(
                buffer_lines=buffer_lines
            )
        self.save_gdf(gdf)

    def run_line_not_intersect_layer(self, buffer_lines=False):
        self.read_data_intersect()
        lines_gdf = self.find_not_intersections_line_on_geom(buffer_lines=buffer_lines)
        self.save_gdf(lines_gdf)

    def run_line_not_touches_layer(self, buffer_lines=False):
        self.read_data_intersect()
        lines_gdf = self.find_not_intersections_touches_on_geom(
            buffer_lines=buffer_lines
        )
        self.save_gdf(lines_gdf)

    def run_feature_not_contains_layer(self):
        self.read_data_intersect()
        gdf = self.find_not_contains_geom()
        self.save_gdf(gdf)


class TopoValidatorTools:
    def prep_output_folder(
        self,
        output_dir=r"c:\data\topoedit\validation-data",
        use_date=True,
        remove_folder=True,
    ):
        if use_date:
            date_str = datetime.datetime.now().strftime("%Y-%m-%d")
            output_dir = os.path.join(output_dir, date_str)

        if remove_folder and os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        os.makedirs(output_dir, exist_ok=True)
        return output_dir

    def get_update_date(self, date=None, weeks=1):
        if date is None:
            date = self.last_week(weeks)
        elif date == "today":
            date = datetime.datetime.now().strftime("%Y-%m-%d")

        # date('2025-09-13');
        where = f"update_date >= date('{date}')"
        return where

    def last_week(self, number_of_weeks=1):
        last_week = datetime.datetime.now() - datetime.timedelta(weeks=number_of_weeks)
        return last_week.strftime("%Y-%m-%d")
