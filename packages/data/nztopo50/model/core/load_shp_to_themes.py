import os
import glob
import pandas as pd # type: ignore
import geopandas as gpd  # type: ignore

from pyogrio import read_info, write_dataframe  # type: ignore
import pyproj
from sqlalchemy import create_engine # type: ignore


class Topo50DataLoader:
    def __init__(
        self, shapefile_dir, excel_file, database, count_log, dataset_field="dataset"
    ):
        self.shapefile_dir = shapefile_dir
        self.dataset_field = dataset_field
        self.excel_file = excel_file
        self.search_path = os.path.join(shapefile_dir, "*.shp")
        self.output = database
        self.layers_info = self._load_layers_info()
        self.layer_groups = {}
        self.common_fields = {}
        self.count_log_file = open(count_log, "w")
        self.count_log_file.write("layer_name, row_count\n")

    def _load_layers_info(self):
        source = pd.read_excel(self.excel_file, sheet_name="Sheet1")
        layers_info = {}
        for row in source.itertuples():
            object_name = row.object_name
            shp_name = row.shp_name
            theme = row.theme
            dataset = row.dataset
            feature_type = row.feature_type
            layer_name = row.layer_name
            layer_list = [object_name, theme, feature_type, layer_name, dataset]
            layers_info[shp_name] = layer_list
        return layers_info

    @staticmethod
    def get_basename(file):
        basename = os.path.basename(file)
        shp_name = basename.split(".")[0]
        if "_" in basename:
            if basename.count("_") > 1:
                basename = basename.split("_")[0] + "_" + basename.split("_")[1]
            else:
                basename = basename.split("_")[0]
        else:
            basename = basename.split(".")[0]
        return shp_name, basename

    @staticmethod
    def write_dataset(
        extension,
        gdf,
        output_file=None,
        layer_name=None,
        dataset_name=None,
        append_data=True,
        schema_name="toposource",
    ):
        try:
            layer_name = layer_name.lower()
            if extension == "geojson":
                write_dataframe(gdf, output_file, driver="GeoJSON")
            elif extension == "shapefile":
                write_dataframe(
                    gdf, output_file, driver="ESRI Shapefile", append=append_data
                )
            elif extension == "gpkg":
                write_dataframe(gdf, output_file, layer=layer_name, driver="GPKG")
            elif extension == "postgis":
                engine = create_engine(
                    "postgresql://postgres:landinformation@localhost:5432/topo"
                )
                schema = dataset_name.lower().replace("_layers", "")
                schema = schema_name
                gdf.to_postgis(
                    name=layer_name,
                    con=engine,
                    schema=schema,
                    if_exists="append" if append_data else "replace",
                    index=False,
                )
            else:
                write_dataframe(
                    gdf,
                    output_file,
                    layer=layer_name,
                    driver="OpenFileGDB",
                    append=append_data,
                    FEATURE_DATASET=dataset_name,
                    TARGET_ARCGIS_VERSION="ARCGIS_PRO_3_2_OR_LATER",
                )
        except Exception as e:
            print(f"Error writing layer '{layer_name}' to '{output_file}': {e}")

    def group_layers(self):
        for file in glob.glob(self.search_path):
            shp_name, basename = self.get_basename(file)
            info = read_info(file)
            layer_info = self.layers_info.get(shp_name, None)
            if layer_info is None:
                print(f"Skipping {shp_name}")
                continue
            layer_name = layer_info[3]
            fields = info["fields"]
            if layer_name not in self.layer_groups:
                self.layer_groups[layer_name] = [fields]
            else:
                self.layer_groups[layer_name].append(fields)

    def compute_common_fields(self):
        for key in self.layer_groups:
            field_sets = self.layer_groups[key]
            first = True
            for fields in field_sets:
                if first:
                    common_columns = set(fields)
                    first = False
                else:
                    common_columns = common_columns | set(fields)
            self.common_fields[key] = common_columns

    def reset_column_names(self, gdf, layer_name):
        # predefine column name changes
        # if "t50_fid" in gdf.columns:
        #    gdf = gdf.drop(columns=["t50_fid"])

        if layer_name.lower() == "tunnel_line":
            if "use1" in gdf.columns:
                gdf = gdf.rename(columns={"use1": "tunnel_use"})
            if "use2" in gdf.columns:
                gdf = gdf.rename(columns={"use2": "tunnel_use2"})
            if "type" in gdf.columns:
                gdf = gdf.rename(columns={"type": "tunnel_type"})

        if layer_name.lower() == "structure":
            gdf = gdf.rename(columns={"type": "structure_type"})

        if layer_name.lower() == "structure_point":
            gdf = gdf.rename(columns={"use": "structure_use"})
            gdf = gdf.rename(columns={"type": "structure_type"})

        if layer_name.lower() == "structure_line":
            gdf = gdf.rename(columns={"wharf_use": "structure_use"})

        if layer_name.lower() == "bridge_line":
            if "use_1" in gdf.columns:
                gdf = gdf.rename(columns={"use_1": "bridge_use"})
            if "use_2" in gdf.columns:
                gdf = gdf.rename(columns={"use_2": "bridge_use2"})

        if layer_name.lower() == "water_point":
            if "temp" in gdf.columns:
                gdf = gdf.rename(columns={"temp": "temperature_indicator"})

        if layer_name.lower() == "landcover":
            if "track_use" in gdf.columns:
                gdf = gdf.rename(columns={"track_use": "landcover_use"})

        if layer_name.lower() == "landcover_line":
            if "track_use" in gdf.columns:
                gdf = gdf.rename(columns={"track_use": "landcover_use"})

        if layer_name.lower() == "landuse":
            if "track_use" in gdf.columns:
                gdf = gdf.rename(columns={"track_use": "landuse_use"})

        if layer_name.lower() == "landuse_line":
            if "track_use" in gdf.columns:
                gdf = gdf.rename(columns={"track_use": "landuse_use"})

        if layer_name.lower() == "water":
            if "lake_use" in gdf.columns:
                gdf = gdf.rename(columns={"lake_use": "water_use"})

        if "compositn" in gdf.columns:
            gdf = gdf.rename(columns={"compositn": "composition"})
        if "descriptn" in gdf.columns:
            gdf = gdf.rename(columns={"descriptn": "description"})
        if "info_disp" in gdf.columns:
            gdf = gdf.rename(columns={"info_disp": "info_display"})
        if "veh_type" in gdf.columns:
            gdf = gdf.rename(columns={"veh_type": "vehicle_type"})
        if layer_name.lower() == "road_line":
            if "hway_num" in gdf.columns:
                gdf = gdf.rename(columns={"hway_num": "highway_number"})
            if "num_lanes" in gdf.columns:
                gdf = gdf.rename(columns={"num_lanes": "lane_count"})
                gdf["lane_count"] = gdf["lane_count"].fillna(0)
                gdf["lane_count"] = gdf["lane_count"].astype(int)
            if "lol_sufi" in gdf.columns:
                gdf = gdf.rename(columns={"lol_sufi": "rna_sufi"})
                gdf["rna_sufi"] = gdf["rna_sufi"].fillna(0)
                gdf["rna_sufi"] = gdf["rna_sufi"].astype(int)
            if "RW_lane_c" in gdf.columns:
                gdf.drop(columns=["RW_lane_c"], inplace=True)
            if "width" in gdf.columns:
                gdf = gdf.rename(columns={"width": "width_indicator"})
            if "RW_surface" in gdf.columns:
                gdf.drop(columns=["RW_surface"], inplace=True)
        if "UFID" in gdf.columns:
            gdf = gdf.rename(columns={"UFID": "t50_fid"})
            gdf["t50_fid"] = gdf["t50_fid"].fillna(0)
            gdf["t50_fid"] = gdf["t50_fid"].astype(int)

        if layer_name.lower() == "island":
            gdf["location"] = gdf["location"].fillna(0)
            gdf["location"] = gdf["location"].astype(int)
        if "temp" in gdf.columns:
            gdf = gdf.rename(columns={"temp": "temperature"})
        if "temperatur" in gdf.columns:
            gdf = gdf.rename(columns={"temperatur": "temperature"})
        if "restrictns" in gdf.columns:
            gdf = gdf.rename(columns={"restrictns": "restrictions"})
        if "orientatn" in gdf.columns:
            gdf = gdf.rename(columns={"orientatn": "orientation"})
        if "constr_typ" in gdf.columns:
            gdf = gdf.rename(columns={"constr_typ": "construction_type"})
        if "support_typ" in gdf.columns:
            gdf = gdf.rename(columns={"support_typ": "support_type"})
        if "support_ty" in gdf.columns:
            gdf = gdf.rename(columns={"support_ty": "support_type"})
        if "bldg_use" in gdf.columns:
            gdf = gdf.rename(columns={"bldg_use": "building_use"})
        if "pipe_use" in gdf.columns:
            gdf = gdf.rename(columns={"pipe_use": "infrastructure_use"})
        if "rway_use" in gdf.columns:
            gdf = gdf.rename(columns={"rway_use": "railway_use"})
        if "embkmt_use" in gdf.columns:
            gdf = gdf.rename(columns={"embkmt_use": "relief_use"})
        if "grp_ascii" in gdf.columns:
            gdf = gdf.rename(columns={"grp_ascii": "group_ascii"})
        if "grp_macron" in gdf.columns:
            gdf = gdf.rename(columns={"grp_macron": "group_macron"})
        if "grp_name" in gdf.columns:
            gdf = gdf.rename(columns={"grp_name": "group_name"})

        return gdf

    def process_and_save_layers(self, target="postgis", schema_name="toposource"):
        processed_layer = []
        for file in glob.glob(self.search_path):
            gdf = gpd.read_file(file)
            # info = read_info(file)
            # if info['geometry_type'] == 'Polygon':
            #    gdf['Shape_Area'] = gdf.geometry.area
            #    gdf['Shape_Length'] = gdf.geometry.length
            # elif info['geometry_type']  == 'LineString':
            #    gdf['Shape_Length'] = gdf.geometry.length
            shp_name, basename = self.get_basename(file)
            layer_info = self.layers_info.get(shp_name, None)
            if layer_info is None:
                continue

            ############# TEMP for testing
            #if layer_info[3].lower() != 'contour':
            #    print(f"Skipping layer: {layer_info[3]}")
            #    continue
            layer_name = layer_info[3]
            # theme = layer_info[1]
            dataset = layer_info[4]
            if layer_name not in processed_layer:
                processed_layer.append(layer_name)
            common_columns = self.common_fields[layer_name]
            for col in common_columns:
                if col not in gdf.columns:
                    gdf[col] = None
            # gdf["theme"] = theme
            gdf["feature_type"] = layer_info[2]

            gdf = gdf.to_crs(epsg=2193)
            cols = [
                col
                for col in common_columns
                if col in gdf.columns and col != gdf.geometry.name
            ]
            cols += [c for c in ["feature_type"] if c in gdf.columns]
            # cols += [c for c in ["theme"] if c in gdf.columns]

            cols.append(gdf.geometry.name)
            gdf = gdf[cols]
            gdf = gdf.reset_index(drop=True)

            gdf = self.reset_column_names(gdf, layer_name)

            print(f"Saving layer: {layer_name} from file: {basename}")
            print(gdf.shape[0], "rows in layer", layer_name)
            self.count_log_file.write(f"{layer_name}, {gdf.shape[0]}\n")

            # test = r"c:\\temp\\data.gpkg"
            # self.write_dataset("gpkg", gdf, test, layer_name, dataset, True)
            if target == "gdb":
                self.write_dataset("gdb", gdf, self.output, layer_name, dataset, True)
                print(f"Layer {layer_name} saved to GDB: {self.output}")
            else:
                self.write_dataset(
                    "postgis", gdf, self.output, layer_name, dataset, True, schema_name
                )

    def run(self):
        print("Starting...")
        # target = "gdb"
        target = "postgis"
        schema_name = self.output
        self.group_layers()
        self.compute_common_fields()
        self.process_and_save_layers(target, schema_name)
        print("Completed...")


if __name__ == "__main__":
    local_app_data = os.getenv("LOCALAPPDATA")
    if local_app_data is None:
        raise ValueError("LOCALAPPDATA environment variable not found")
    folder = os.path.join(
        local_app_data,
        "miniconda3",
        "pkgs",
        "proj-9.6.2-h4f671f6_0",
        "Library",
        "share",
        "proj",
    )
    pyproj.datadir.set_data_dir(folder)
    layer_info_file = r"C:\Data\Model\layers_info.xlsx"

    # release = "release62"
    release = "release64"
    data_folder = rf"C:\Data\Topo50\{release}_NZ50_Shape"

    # postgis schema name
    database = release  # r"C:\Data\Model\geodatabase\topo50data_themes.gdb"
    count_log = r"C:\Data\Model\count_log.txt"

    loader = Topo50DataLoader(
        shapefile_dir=data_folder,
        excel_file=layer_info_file,
        database=database,
        count_log=count_log,
    )
    loader.run()
