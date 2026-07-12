import csv
from argparse import ArgumentParser

from qgis.core import (
    QgsApplication,
    QgsFeature,
    QgsProject,
)

from utils.mag_info import calculate_mag_info, render_mag_info

# from root: (topographic-system)
# 1. npm run build && npm run bundle
# 2. docker build -f packages/map/Dockerfile -t map .
# 3. docker run -it --rm -u $(id -u):$(id -g) -v "${PWD}:${PWD}" -w "${PWD}" --entrypoint uv map run --no-project packages/qgis/src/utils/mag_info_to_csv.py --qgis-project-path ./qgis/nztopo50.qgs
# 4. the qgis project file and parquet files must live under PWD, such as "${PWD}/qgis/". otherwise, they're not accessible.

parser = ArgumentParser()
parser.add_argument("--qgis-project-path", required=True, type=str)
parser.add_argument("--topo-map-sheet-name", default="nztopo50_map_sheet", type=str)

if __name__ == "__main__":
    args = parser.parse_args()

    qgis_project_path = args.qgis_project_path
    topo_map_sheet_name = args.topo_map_sheet_name

    QgsApplication.setPrefixPath("/usr", True)
    qgs = QgsApplication([], False)

    try:
        qgs.initQgis()
        print("QGIS initialized")

        project = QgsProject.instance()

        if not project.read(qgis_project_path):
            raise ValueError(f"Failed to read QGIS project: {qgis_project_path}")

        matching_layers = project.mapLayersByName(topo_map_sheet_name)
        if not matching_layers:
            raise ValueError(f"No layer found with name '{topo_map_sheet_name}'.")
        topo_sheet_layer = matching_layers[0]

        features = list(topo_sheet_layer.getFeatures())
        features_sorted = sorted(features, key=lambda f: f["sheet_code"])

        csv_filename = "mag_info"
        with open(f"{csv_filename}.csv", "w", newline="", encoding="utf-8") as file:
            writer_conv = csv.writer(file, delimiter="\t")
            writer_conv.writerow(["sheet_code", "gm_degrees", "gm_mils", "gm_year", "gm_rate_years"])

            for feature in features_sorted:
                if not isinstance(feature, QgsFeature):
                    raise TypeError("feature is not a QgsFeature")

                sheet_code = feature.attribute("sheet_code")

                # handle magnetic info
                mag_info_raw = calculate_mag_info(project, feature, topo_sheet_layer.crs())
                mag_info_render = render_mag_info(mag_info_raw)

                writer_conv.writerow(
                    [
                        sheet_code,
                        mag_info_render["gm_degrees"],
                        mag_info_render["gm_mils"],
                        mag_info_render["gm_year"],
                        mag_info_render["gm_rate_years"],
                    ]
                )
    finally:
        qgs.exitQgis()
        print("QGIS exited")
