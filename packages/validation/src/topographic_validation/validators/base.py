import os

import geopandas as gpd
from geopandas.sindex import SpatialIndex
from shapely import Point
import datetime
from abc import ABC, abstractmethod


class AbstractTopologyValidator(ABC):
    summary_report: dict[str, bool | str]
    export_validation_data: bool
    db_url: str
    table: str
    table2: str
    mode: str
    layername: str
    where_condition: str | None
    output_dir: str
    area_crs: int
    message: str
    gdf: gpd.GeoDataFrame
    sindex: SpatialIndex | None
    gdf2: gpd.GeoDataFrame
    bbox: tuple[float, float, float, float] | None
    export_parquet: bool
    export_parquet_by_geometry_type: bool
    export_gpkg: bool
    pkey: str
    source: str
    geom_column: str

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
        message: str = "validation error",
        output_dir: str = "./topoedit/validation-data",
        area_crs: int = 2193,
    ) -> None:
        self.summary_report = summary_report
        self.export_validation_data = export_validation_data
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
        self.gdf = gpd.GeoDataFrame()
        self.sindex = None
        self.gdf2 = gpd.GeoDataFrame()
        self.bbox = bbox
        self.set_exports(True, False, True)

    @property
    def twotable(self) -> bool:
        return self.table2 is not None and self.table != self.table2

    def set_exports(
        self,
        export_parquet: bool = True,
        export_parquet_by_geometry_type: bool = True,
        export_gpkg: bool = True,
    ) -> None:
        self.export_parquet = export_parquet
        self.export_parquet_by_geometry_type = export_parquet_by_geometry_type
        self.export_gpkg = export_gpkg

    def get_names(self, gdf: gpd.GeoDataFrame, idx1: int, idx2: int) -> tuple[str, str]:
        if hasattr(gdf, "name"):
            return gdf.name.iloc[idx1], gdf.name.iloc[idx2]
        else:
            return "noname", "noname"

    def get_feature_types(
        self, gdf: gpd.GeoDataFrame, idx1: int, idx2: int
    ) -> tuple[str, str]:
        if hasattr(gdf, "feature_type"):
            return gdf.feature_type.iloc[idx1], gdf.feature_type.iloc[idx2]
        else:
            return "nofeaturetype", "nofeaturetype"

    def get_keys(
        self, gdf: gpd.GeoDataFrame, idx1: int, idx2: int
    ) -> tuple[int | str, int | str]:
        return gdf[self.pkey].iloc[idx1], gdf[self.pkey].iloc[idx2]

    def get_valid_geometries(self, geometries: list[dict]) -> list[dict]:
        all_intersection_geometries_valid: list[dict] = []
        for item in geometries:
            geom = item["geometry"]
            if isinstance(geom, str):
                continue
            if geom.is_valid:
                all_intersection_geometries_valid.append(item)
            else:
                continue
        return all_intersection_geometries_valid

    @staticmethod
    def get_first_last(geom) -> tuple[Point | None, Point | None]:
        if geom.geom_type == "LineString":
            return Point(geom.coords[0]), Point(geom.coords[-1])
        elif geom.geom_type == "MultiLineString":
            first_line = geom.geoms[0]
            last_line = geom.geoms[-1]
            return Point(first_line.coords[0]), Point(last_line.coords[-1])
        else:
            return None, None

    @staticmethod
    def split_first_two_spaces(s: str) -> tuple[str, str, str]:
        """Find the first and second space and split into tuple of 3 objects"""
        first = s.find(" ")
        second = s.find(" ", first + 1)
        if first == -1 or second == -1:
            return s, "", ""
        return s[:first], s[first + 1 : second], s[second + 1 :]

    @abstractmethod
    def _read_data(self) -> None:
        """Read the main dataset(s) - to be implemented by concrete classes"""
        pass

    @abstractmethod
    def _read_data_by_rule(self, rule_is_null: bool = True, rule: str = "") -> None:
        """Read dataset filtered by a rule - to be implemented by concrete classes"""
        pass

    def read_datasets(self) -> None:
        """Public method to read datasets"""
        self._read_data()

        if self.twotable:
            self.sindex = self.gdf2.sindex
        else:
            self.sindex = self.gdf.sindex

    def read_dataset_by_rule(self, rule_is_null: bool = True, rule: str = "") -> None:
        """Public method to read dataset by rule"""
        self._read_data_by_rule(rule_is_null, rule)
        self.sindex = self.gdf.sindex

    def find_self_intersecting(
        self,
    ) -> tuple[bool, list[dict], list[dict], list[dict], list[dict], list[str]]:
        if self.sindex is None:
            raise ValueError("Spatial index is not initialized.")

        candidate_pairs = set()
        starttime = datetime.datetime.now()

        for label, row in self.gdf.iterrows():
            idx1 = self.gdf.index.get_loc(label)
            if not isinstance(idx1, int):
                raise ValueError("Duplicate index labels are not supported")
            geom = row.get("geom", None) or row.get("geometry", None)
            if not geom:
                raise ValueError("Geometry column not found in GeoDataFrame")

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
            if self.twotable:
                geom2 = self.gdf2.geometry.iloc[idx2]
            else:
                geom2 = self.gdf.geometry.iloc[idx2]
            if geom1.intersects(geom2):
                intersection_geom = geom1.intersection(geom2)
                geomtypes.append(intersection_geom.geom_type)
                if not intersection_geom.is_empty:
                    original_names = self.get_names(self.gdf, idx1, idx2)
                    original_keys = self.get_keys(self.gdf, idx1, idx2)
                    original_feature_types = self.get_feature_types(
                        self.gdf, idx1, idx2
                    )
                    if intersection_geom.geom_type in ["Point", "MultiPoint"]:
                        intersection_geometries_point.append(
                            {
                                "geometry": intersection_geom,
                                "pair_names": f"{original_names[0]}-{original_names[1]}",
                                "pair_keys": f"{original_keys[0]}-{original_keys[1]}",
                                "pair_feature_types": f"{original_feature_types[0]}-{original_feature_types[1]}",
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
                                "pair_feature_types": f"{original_feature_types[0]}-{original_feature_types[1]}",
                            }
                        )
                    elif intersection_geom.geom_type == "GeometryCollection":
                        for geom_type, entry in self.handle_geometry_collection(
                            intersection_geom, original_names, original_feature_types
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
                                "pair_feature_types": f"{original_feature_types[0]}-{original_feature_types[1]}",
                            }
                            intersection_geometries_multipolygon.append(entry)
                    else:
                        intersection_geometries.append(
                            {
                                "geometry": intersection_geom,
                                "pair_names": f"{original_names[0]}-{original_names[1]}",
                                "pair_keys": f"{original_keys[0]}-{original_keys[1]}",
                                "pair_feature_types": f"{original_feature_types[0]}-{original_feature_types[1]}",
                            }
                        )

        has_validation_errors = (
            len(intersection_geometries) > 0
            or len(intersection_geometries_point) > 0
            or len(intersection_geometries_line) > 0
            or len(intersection_geometries_multipolygon) > 0
        )
        return (
            has_validation_errors,
            intersection_geometries,
            intersection_geometries_point,
            intersection_geometries_line,
            intersection_geometries_multipolygon,
            geomtypes,
        )

    def handle_geometry_collection(
        self,
        intersection_geom,
        original_names: tuple[str, str],
        original_feature_types: tuple[str, str],
    ) -> list[tuple[str, dict]]:
        geoms: list[tuple[str, dict]] = []
        for geom in intersection_geom.geoms:
            entry = {
                "geometry": self.geom_column,
                "pair_names": f"{original_names[0]}-{original_names[1]}",
                "pair_feature_types": f"{original_feature_types[0]}-{original_feature_types[1]}",
            }
            geoms.append((geom.geom_type, entry))
        return geoms

    def find_intersections_features_between_layers(self) -> gpd.GeoDataFrame:
        starttime = datetime.datetime.now()

        intersecting_features = gpd.sjoin(self.gdf, self.gdf2, how="inner")
        intersecting_features.columns = [
            col.replace("_left", "") for col in intersecting_features.columns
        ]
        columns: list[str] = [self.pkey, self.geom_column]
        if self.pkey != "topo_id":
            columns.append("topo_id")
        if "name" in intersecting_features.columns:
            columns.append("name")

        intersecting_features = intersecting_features[columns]
        if not intersecting_features.empty:
            self.update_summary_report("feature_in_layers")

        endtime = datetime.datetime.now()
        print("Time taken for spatial index query:", endtime - starttime)

        return intersecting_features

    def find_not_intersections_features_between_layers(
        self, predicate: str = "intersects", buffer_lines: bool = True
    ) -> gpd.GeoDataFrame:
        if self.gdf.empty or self.gdf2.empty:
            return self.gdf

        if buffer_lines and self.gdf2.geom_type.iloc[0] in [
            "LineString",
            "MultiLineString",
        ]:
            buffer_distance = 0.000001
            self.gdf2[self.geom_column] = self.gdf2.geometry.buffer(buffer_distance)  # TODO: Should this be in 2193? Note that gdf2 is being mutated here.

        intersecting_features = gpd.sjoin(
            self.gdf, self.gdf2, how="left", predicate=predicate
        )
        non_intersecting_features = intersecting_features[
            intersecting_features["index_right"].isna()
        ]
        non_intersecting_features.columns = [
            col.replace("_left", "") for col in non_intersecting_features.columns
        ]
        wanted = [self.pkey, self.geom_column, "topo_id", "name"]
        columns = list(dict.fromkeys(col for col in wanted if col in non_intersecting_features.columns))

        non_intersecting_features = non_intersecting_features[columns]

        return non_intersecting_features

    def update_summary_report(self, rule: str) -> None:
        self.summary_report[rule] = True

    def save_gdf(
        self,
        gdf: gpd.GeoDataFrame,
        validation_type: str = "topology",
        extended_name: str = "",
    ) -> None:
        if self.export_validation_data is False:
            return
        if gdf.empty:
            print(f"No validation errors found for {self.table}.")
            return
        if len(extended_name) > 0:
            extended_name = f"_{extended_name}"

        gdf["warning"] = self.message
        gdf["open"] = True
        gdf["val_date"] = datetime.datetime.now().strftime("%Y-%m-%d")
        gdf["notes"] = ""

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
        intersection_geometries: list[dict],
        intersection_geometries_point: list[dict],
        intersection_geometries_line: list[dict],
        intersection_geometries_multipolygon: list[dict],
    ) -> None:
        if self.export_validation_data is False:
            return
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
        all_intersection_geometries: list[dict] = []

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
            intersections_gdf["open"] = True
            intersections_gdf["val_date"] = validation_date
            intersections_gdf["notes"] = ""

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
                intersections_gdf["warning"] = self.message
                intersections_gdf["open"] = True
                intersections_gdf["val_date"] = validation_date
                intersections_gdf["notes"] = ""
                intersections_gdf["Area"] = projected_gdf.geometry.area
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
                intersections_gdf["open"] = True
                intersections_gdf["val_date"] = validation_date
                intersections_gdf["notes"] = ""

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
                intersections_gdf["open"] = True
                intersections_gdf["val_date"] = validation_date
                intersections_gdf["notes"] = ""

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
                intersections_gdf["open"] = True
                intersections_gdf["val_date"] = validation_date
                intersections_gdf["notes"] = ""

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

    def run_self_intersections(self, rule_name: str = "") -> None:
        starttime = datetime.datetime.now()

        self.read_datasets()
        (
            has_validation_errors,
            intersection_geometries,
            intersection_geometries_point,
            intersection_geometries_line,
            intersection_geometries_multipolygon,
            geomtypes,
        ) = self.find_self_intersecting()
        print("Unique intersection geometry types:", list(set(geomtypes)))

        if has_validation_errors:
            self.update_summary_report("self_intersect_layers")
        self.save_intersection_outputs(
            intersection_geometries,
            intersection_geometries_point,
            intersection_geometries_line,
            intersection_geometries_multipolygon,
        )
        print("Time taken for read_datasets:", datetime.datetime.now() - starttime)

    def run_layer_intersections(
        self,
        rule_name: str = "",
        intersect: bool = True,
        buffer_lines: bool = True,
        predicate: str = "intersects",
    ) -> None:
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

        if not self.gdf.empty:
            self.update_summary_report(rule_name)
        self.save_gdf(
            gdf, validation_type=val_type, extended_name=self.table2.replace(".", "_")
        )
        print(
            "Time taken to process layer intersections:",
            datetime.datetime.now() - starttime,
        )

    def run_null_column_checks(
        self, rule_name: str = "", column_name: str = ""
    ) -> None:
        starttime = datetime.datetime.now()

        self.read_dataset_by_rule(rule_is_null=True, rule=column_name)
        if not self.gdf.empty:
            self.update_summary_report(rule_name)
        self.save_gdf(self.gdf, validation_type="null", extended_name=column_name)
        print(
            "Time taken for process null column check:",
            datetime.datetime.now() - starttime,
        )

    def run_query_rule_checks(
        self, rule_name: str, rule: str, column_name: str
    ) -> None:
        starttime = datetime.datetime.now()

        self.read_dataset_by_rule(rule_is_null=False, rule=rule)
        if not self.gdf.empty:
            self.update_summary_report(rule_name)
        self.save_gdf(self.gdf, validation_type="query", extended_name=column_name)
        print(
            "Time taken for process query rule check:",
            datetime.datetime.now() - starttime,
        )
