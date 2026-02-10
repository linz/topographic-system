import datetime
import os
import shutil
import json


class TopoValidatorTools:
    def prep_output_folder(
        self,
        output_dir: str = "./topoedit/validation-data",
        use_date: bool = True,
        remove_folder: bool = True,
    ) -> str:
        if use_date:
            date_str = datetime.datetime.now().strftime("%Y-%m-%d")
            output_dir = os.path.join(output_dir, date_str)

        if remove_folder and os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        os.makedirs(output_dir, exist_ok=True)
        return output_dir

    def create_folder(self, path: str) -> None:
        if not os.path.exists(path):
            os.makedirs(path)

    def get_update_date(self, date: str | None = None, weeks: int = 1) -> str:
        if date is None:
            date = self.last_week(weeks)
        elif date == "today":
            date = datetime.datetime.now().strftime("%Y-%m-%d")

        # date('2025-09-13');
        where = f"update_date >= date('{date}')"
        return where

    def last_week(self, number_of_weeks: int = 1) -> str:
        last_week = datetime.datetime.now() - datetime.timedelta(weeks=number_of_weeks)
        return last_week.strftime("%Y-%m-%d")


class TopoValidatorSettings:
    def __init__(
        self,
        validation_config_file: str | None = None,
        db_path: str | None = None,
        output_dir: str | None = None,
        area_crs: int = 2193,
        export_validation_data: bool = True,
        export_parquet: bool = False,
        export_parquet_by_geometry_type: bool = False,
        export_gpkg: bool = True,
        use_date_folder: bool = False,
        process_queries: bool = True,
        process_features_on_layer: bool = True,
        process_self_intersections: bool = True,
        update_date: str | None = None,
        weeks: int | None = None,
        bbox: tuple[float, float, float, float] | None = None,
    ) -> None:
        self.validation_config_file = validation_config_file
        self.db_path = db_path
        self.output_dir = output_dir
        self.area_crs = area_crs
        self.export_validation_data = export_validation_data
        self.export_parquet = export_parquet
        self.export_parquet_by_geometry_type = export_parquet_by_geometry_type
        self.export_gpkg = export_gpkg
        self.use_date_folder = use_date_folder
        self.process_queries = process_queries
        self.process_features_on_layer = process_features_on_layer
        self.process_self_intersections = process_self_intersections
        self.update_date = update_date
        self.weeks = weeks
        self.bbox = bbox

    def load_validation_config(self) -> None:
        with open(self.validation_config_file, "r") as f:
            loaded_data = json.load(f)
            self.feature_not_on_layers = loaded_data.get("feature_not_on_layers", [])
            self.feature_in_layers = loaded_data.get("feature_in_layers", [])
            self.line_not_on_feature_layers = loaded_data.get(
                "line_not_on_feature_layers", []
            )
            self.line_not_touches_feature_layers = loaded_data.get(
                "line_not_touches_feature_layers", []
            )
            self.feature_not_contains_layers = loaded_data.get(
                "feature_not_contains_layers", []
            )
            self.self_intersect_layers = loaded_data.get("self_intersect_layers", [])
            self.null_columns = loaded_data.get("null_columns", [])
            self.query_rules = loaded_data.get("query_rules", [])

    @property
    def validation_config_file(self):
        return self._validation_config_file

    @validation_config_file.setter
    def validation_config_file(self, value):
        self._validation_config_file = value
        if value:
            self.load_validation_config()

    @property
    def db_path(self):
        return self._db_path

    @db_path.setter
    def db_path(self, value):
        self._db_path = value

    @property
    def output_dir(self):
        return self._output_dir

    @output_dir.setter
    def output_dir(self, value):
        self._output_dir = value

    @property
    def area_crs(self):
        return self._area_crs

    @area_crs.setter
    def area_crs(self, value):
        self._area_crs = value

    @property
    def export_parquet(self):
        return self._export_parquet

    @export_parquet.setter
    def export_parquet(self, value):
        self._export_parquet = value

    @property
    def export_parquet_by_geometry_type(self):
        return self._export_parquet_by_geometry_type

    @export_parquet_by_geometry_type.setter
    def export_parquet_by_geometry_type(self, value):
        self._export_parquet_by_geometry_type = value

    @property
    def export_gpkg(self):
        return self._export_gpkg

    @export_gpkg.setter
    def export_gpkg(self, value):
        self._export_gpkg = value

    @property
    def use_date_folder(self):
        return self._use_date_folder

    @use_date_folder.setter
    def use_date_folder(self, value):
        self._use_date_folder = value

    @property
    def process_queries(self):
        return self._process_queries

    @process_queries.setter
    def process_queries(self, value):
        self._process_queries = value

    @property
    def process_features_on_layer(self):
        return self._process_features_on_layer

    @process_features_on_layer.setter
    def process_features_on_layer(self, value):
        self._process_features_on_layer = value

    @property
    def process_self_intersections(self):
        return self._process_self_intersections

    @process_self_intersections.setter
    def process_self_intersections(self, value):
        self._process_self_intersections = value

    @property
    def feature_not_on_layers(self):
        return self._feature_not_on_layers

    @feature_not_on_layers.setter
    def feature_not_on_layers(self, value):
        self._feature_not_on_layers = value

    @property
    def feature_in_layers(self):
        return self._feature_in_layers

    @feature_in_layers.setter
    def feature_in_layers(self, value):
        self._feature_in_layers = value

    @property
    def line_not_on_feature_layers(self):
        return self._line_not_on_feature_layers

    @line_not_on_feature_layers.setter
    def line_not_on_feature_layers(self, value):
        self._line_not_on_feature_layers = value

    @property
    def line_not_touches_feature_layers(self):
        return self._line_not_touches_feature_layers

    @line_not_touches_feature_layers.setter
    def line_not_touches_feature_layers(self, value):
        self._line_not_touches_feature_layers = value

    @property
    def feature_not_contains_layers(self):
        return self._feature_not_contains_layers

    @feature_not_contains_layers.setter
    def feature_not_contains_layers(self, value):
        self._feature_not_contains_layers = value

    @property
    def self_intersect_layers(self):
        return self._self_intersect_layers

    @self_intersect_layers.setter
    def self_intersect_layers(self, value):
        self._self_intersect_layers = value

    @property
    def null_columns(self):
        return self._null_columns

    @null_columns.setter
    def null_columns(self, value):
        self._null_columns = value

    @property
    def query_rules(self):
        return self._query_rules

    @query_rules.setter
    def query_rules(self, value):
        self._query_rules = value

    @property
    def update_date(self):
        return self._update_date

    @update_date.setter
    def update_date(self, value):
        self._update_date = value

    @property
    def weeks(self):
        return self._weeks

    @weeks.setter
    def weeks(self, value):
        self._weeks = value

    @property
    def bbox(self):
        return self._bbox

    @bbox.setter
    def bbox(self, value):
        self._bbox = value
