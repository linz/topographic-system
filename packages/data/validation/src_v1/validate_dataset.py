import os
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine
from shapely import Point
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
        self.gdf = None
        self.sindex = None
        self.gdf2 = None
        self.bbox = bbox
        self.set_exports(True, False, True)
        if db_url.startswith("postgresql"):
            self.engine = create_engine(db_url)
            self.pkey = self.get_primary_key()
            self.source = "postgis"
            self.geom_column = "geom"
        elif db_url.endswith(".gpkg"):
            self.pkey = "topo_id"
            self.source = "gpkg"
            self.geom_column = "geometry"
        elif db_url.endswith(".parquet"):
            self.db_url = db_url.replace("files.parquet", "")
            self.pkey = "topo_id"
            self.source = "parquet"
            self.geom_column = "geom"
            self.gdf = None
            self.sindex = None
            self.gdf2 = None
        else:
            raise ValueError(
                "db_url must be a PostgreSQL connection string, a GeoPackage file path, or a Parquet file path."
            )

    def set_exports(
        self,
        export_parquet=True,
        export_parquet_by_geometry_type=True,
        export_gpkg=True,
    ):
        self.export_parquet = export_parquet
        self.export_parquet_by_geometry_type = export_parquet_by_geometry_type
        self.export_gpkg = export_gpkg

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

    def get_valid_geometries(self, geometries):
        all_intersection_geometries_valid = []
        for item in geometries:
            geom = item["geometry"]
            if isinstance(geom, str):
                continue
            if geom.is_valid:
                all_intersection_geometries_valid.append(item)
            else:
                continue
        return all_intersection_geometries_valid

    def get_first_last(geom):
        if geom.geom_type == "LineString":
            return Point(geom.coords[0]), Point(geom.coords[-1])
        elif geom.geom_type == "MultiLineString":
            first_line = geom.geoms[0]
            last_line = geom.geoms[-1]
            return Point(first_line.coords[0]), Point(last_line.coords[-1])
        else:
            return None, None

    def _read_data_gpkg(self):
        if self.where_condition:
            self.gdf = gpd.read_file(
                self.db_url, layer=self.table, where=self.where_condition
            )
        elif self.bbox:
            self.gdf = gpd.read_file(self.db_url, layer=self.table, bbox=self.bbox)
        else:
            self.gdf = gpd.read_file(self.db_url, layer=self.table)

        if self.mode == "twotable":
            if self.bbox:
                self.gdf2 = gpd.read_file(
                    self.db_url, layer=self.table2, bbox=self.bbox
                )
            else:
                self.gdf2 = gpd.read_file(self.db_url, layer=self.table2)

    # Find the first and second space and split into tuple of 3 objects
    def split_first_two_spaces(s):
        first = s.find(" ")
        second = s.find(" ", first + 1)
        if first == -1 or second == -1:
            return (s, "", "")
        return (s[:first], s[first + 1 : second], s[second + 1 :])

    # Example usage:
    # result = split_first_two_spaces("foo bar baz qux")
    # result -> ('foo', 'bar', 'baz qux')

    # Note: read_paruquet does not support where directly
    def _read_data_parquet(self):
        file = os.path.join(self.db_url, f"{self.table}.parquet")
        if self.where_condition:
            where = (
                self.where_condition.replace("=", "==")
                .replace("AND", "&")
                .replace("OR", "|")
            )
            gdf = gpd.read_parquet(file)
            self.gdf = gdf.query(where)
        elif self.bbox:
            self.gdf = gpd.read_parquet(file, bbox=self.bbox)
        else:
            self.gdf = gpd.read_parquet(file)

        if self.mode == "twotable":
            file = os.path.join(self.db_url, f"{self.table2}.parquet")
            if self.bbox:
                self.gdf2 = gpd.read_parquet(file, bbox=self.bbox)
            else:
                self.gdf2 = gpd.read_parquet(file)

    def _read_data_postgis(self):
        where_condition = ""
        if self.where_condition:
            where_condition = f"WHERE {self.where_condition}"
        elif self.bbox:
            where_condition = f"WHERE {self.geom_column} && ST_MakeEnvelope({self.bbox[0]}, {self.bbox[1]}, {self.bbox[2]}, {self.bbox[3]}, 2193)"
        query = f"SELECT * FROM {self.table} {where_condition}"
        self.gdf = gpd.read_postgis(query, self.engine, geom_col=self.geom_column)

        if self.mode == "twotable":
            self.gdf2 = gpd.read_postgis(
                f"SELECT * FROM {self.table2}", self.engine, geom_col=self.geom_column
            )

    def read_datasets(self):
        if self.source == "gpkg":
            self._read_data_gpkg()
        elif self.source == "parquet":
            self._read_data_parquet()
        else:
            self._read_data_postgis()

        if self.mode == "onetable":
            self.sindex = self.gdf.sindex
        else:
            self.sindex = self.gdf2.sindex

    def read_dataset_by_rule(self, rule_is_null=True, rule=""):
        if self.where_condition:
            where_condition = f" AND {self.where_condition}"
        else:
            where_condition = ""

        if rule_is_null:
            column_name = rule
            where = f"{column_name} IS NULL {where_condition}"
            if self.source == "postgis":
                where = f"WHERE {column_name} IS NULL {where_condition}"
        else:
            where = f"{rule} {where_condition}"
            if self.source == "postgis":
                where = f"WHERE {rule} {where_condition}"

        if self.source == "gpkg":
            self.gdf = gpd.read_file(
                self.db_url,
                layer=self.table,
                columns=[self.pkey, self.geom_column],
                where=where,
            )
        elif self.source == "parquet":
            file = os.path.join(self.db_url, f"{self.table}.parquet")
            gdf = gpd.read_parquet(file)
            if rule_is_null:
                null_gdf = gdf[gdf[column_name].isna()]
            else:
                null_gdf = gdf

            if self.where_condition:
                # "feature_type == 'waterfall' | feature_type == 'waterfall_edge'"
                # "where": "(feature_type = 'waterfall' OR feature_type = 'waterfall_edge')",
                if "OR" in where or "AND" in where:
                    where = where.replace("(", "").replace(")", "")
                where = (
                    self.where_condition.replace("=", "==")
                    .replace("AND", "&")
                    .replace("OR", "|")
                )

                where = (
                    where.replace("IN", "in").replace("NOT", "not").replace("IS", "is")
                )
                if "is not null" in where:
                    where = "~" + where.replace("is not null", ".isnull()")
                elif "is null" in where:
                    where = where.replace("is null", ".isnull()")

                if " in " in where:
                    where = where.replace("(", "[").replace(")", "]")

                null_gdf = null_gdf.query(where)
                self.gdf = null_gdf[[self.pkey, self.geom_column]]
            else:
                self.gdf = null_gdf[[self.pkey, self.geom_column]]
        else:
            query = f"SELECT {self.pkey}, {self.geom_column} FROM {self.table} {where}"
            self.gdf = gpd.read_postgis(query, self.engine, geom_col=self.geom_column)
        self.sindex = self.gdf.sindex

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

    def handle_geometry_collection(self, intersection_geom, original_names):
        geoms = []
        for geom in intersection_geom.geoms:
            entry = {
                "geometry": self.geom_column,
                "pair_names": f"{original_names[0]}-{original_names[1]}",
            }
            geoms.append((geom.geom_type, entry))
        return geoms

    def find_intersections_features_between_layers(self):
        starttime = datetime.datetime.now()

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

    def find_not_intersections_features_between_layers(
        self, predicate="intersects", buffer_lines=True
    ):
        if buffer_lines and self.gdf2.geom_type.iloc[0] in [
            "LineString",
            "MultiLineString",
        ]:
            buffer_lines = 0.000001
            self.gdf2[self.geom_column] = self.gdf2.geometry.buffer(buffer_lines)

        intersecting_features = gpd.sjoin(
            self.gdf, self.gdf2, how="left", predicate=predicate
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

    def find_multipart_geometries(self):
        multipart_polygons_gdf = self.gdf[self.gdf.geom_type == "MultiPolygon"]
        return multipart_polygons_gdf

    def save_gdf(self, gdf, validation_type="topology", extended_name=""):
        if gdf.empty:
            print(f"No validation errors found. {self.table} is empty.")
            return
        if len(extended_name) > 0:
            extended_name = f"_{extended_name}"

        gdf["warning"] = self.message
        gdf["status"] = "open"
        gdf["val_date"] = datetime.datetime.now().strftime("%Y-%m-%d")

        # gdf.set_crs(epsg=4167, inplace=True, allow_override=True)

        if self.export_parquet:
            export_file = os.path.join(
                self.output_dir,
                f"{self.layername}_{validation_type}{extended_name}.parquet",
            )
            gdf.to_parquet(
                f"{export_file}",
                engine="pyarrow",
                compression="zstd",
                write_covering_bbox=True,
                row_group_size=50000,
            )
        if self.export_gpkg:
            export_file = os.path.join(
                self.output_dir, f"topology_{validation_type}.gpkg"
            )
            layer_name = f"{self.layername}_{validation_type}{extended_name}"
            gdf.to_file(f"{export_file}", layer=layer_name, driver="GPKG", append=True)

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
                all_intersection_geometries, crs=self.gdf.crs
            )
            intersections_gdf["warning"] = self.message
            intersections_gdf["status"] = "open"
            intersections_gdf["val_date"] = validation_date
            intersections_gdf.to_parquet(
                rf"{self.output_dir}\{self.layername}_topology_self_intersect.parquet",
                engine="pyarrow",
                compression="zstd",
                write_covering_bbox=True,
                row_group_size=50000,
            )

        if self.export_gpkg or self.export_parquet_by_geometry_type:
            if len(intersection_geometries) > 0:
                intersections_gdf = gpd.GeoDataFrame(
                    intersection_geometries, geometry="geometry", crs=self.gdf.crs
                )
                projected_gdf = intersections_gdf.to_crs(epsg=self.area_crs)
                intersections_gdf["Area"] = projected_gdf.geometry.area
                intersections_gdf["warning"] = self.message
                intersections_gdf["status"] = "open"
                intersections_gdf["val_date"] = validation_date
                if self.export_gpkg:
                    intersections_gdf.to_file(
                        rf"{self.output_dir}\topology_self_intersect.gpkg",
                        layer=f"{self.layername}_errors_areas",
                        driver="GPKG",
                    )
                if self.export_parquet_by_geometry_type:
                    intersections_gdf.to_parquet(
                        rf"{self.output_dir}\{self.layername}_topology_self_intersect_poly.parquet",
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
                        rf"{self.output_dir}\topology_self_intersect.gpkg",
                        layer=f"{self.layername}_errors_points",
                        driver="GPKG",
                    )
                if self.export_parquet_by_geometry_type:
                    intersections_gdf.to_parquet(
                        rf"{self.output_dir}\{self.layername}_topology_self_intersect_point.parquet",
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
                        rf"{self.output_dir}\topology_self_intersect.gpkg",
                        layer=f"{self.layername}_errors_lines",
                        driver="GPKG",
                    )
                if self.export_parquet_by_geometry_type:
                    intersections_gdf.to_parquet(
                        rf"{self.output_dir}\{self.layername}_topology_self_intersect_line.parquet",
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
                        rf"{self.output_dir}\topology_self_intersect.gpkg",
                        layer=f"{self.layername}_errors_multipolygon",
                        driver="GPKG",
                    )
                if self.export_parquet_by_geometry_type:
                    intersections_gdf.to_parquet(
                        rf"{self.output_dir}\{self.layername}_topology_self_intersect_multipolygon.parquet",
                        engine="pyarrow",
                        compression="zstd",
                        write_covering_bbox=True,
                        row_group_size=50000,
                    )

    def run_self_intersections(self):
        starttime = datetime.datetime.now()

        self.read_datasets()
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
        print("Time taken for read_datasets:", datetime.datetime.now() - starttime)

    def run_layer_intersections(
        self, intersect=True, buffer_lines=True, predicate="intersects"
    ):
        starttime = datetime.datetime.now()

        self.read_datasets()

        if intersect:
            gdf = self.find_intersections_features_between_layers()
            val_type = "intersect"
        else:
            gdf = self.find_not_intersections_features_between_layers(
                predicate=predicate, buffer_lines=buffer_lines
            )
            val_type = "not_intersect"
        self.save_gdf(
            gdf, validation_type=val_type, extended_name=self.table2.replace(".", "_")
        )
        print(
            "Time taken to process layer intersections:",
            datetime.datetime.now() - starttime,
        )

    def run_null_column_checks(self, column_name):
        starttime = datetime.datetime.now()

        self.read_dataset_by_rule(rule_is_null=True, rule=column_name)
        self.save_gdf(self.gdf, validation_type="null", extended_name=column_name)
        print(
            "Time taken for process null column check:",
            datetime.datetime.now() - starttime,
        )

    def run_query_rule_checks(self, rule, column_name):
        starttime = datetime.datetime.now()

        self.read_dataset_by_rule(rule_is_null=False, rule=rule)
        self.save_gdf(self.gdf, validation_type="query", extended_name=column_name)
        print(
            "Time taken for process query rule check:",
            datetime.datetime.now() - starttime,
        )

    def run_multipart_geometry_checks(self):
        starttime = datetime.datetime.now()

        self.read_datasets()
        multipart_geometries = self.find_multipart_geometries()
        self.save_gdf(
            multipart_geometries,
            validation_type="multipart geometries",
            extended_name="geometry",
        )
        print(
            "Time taken for process multipart geometry check:",
            datetime.datetime.now() - starttime,
        )


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
