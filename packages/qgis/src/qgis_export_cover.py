from qgis.core import (
    QgsApplication,
    QgsProject,
    QgsLayoutExporter,
    QgsCoordinateTransform,
    QgsLayoutItemMap,
)
import sys
import os
import json

os.environ.update({"QT_QPA_PLATFORM": "offscreen"})

project_path = sys.argv[1]
project_layout = sys.argv[2]
topo_map_sheet = sys.argv[3]
list_all_sheet_codes = sys.argv[4].lower() in ("true", "1", "yes")
sheet_codes = sys.argv[5:]

QgsApplication.setPrefixPath("/usr", True)  # Adjust path as needed
qgs = QgsApplication([], False)  # False = no GUI
qgs.initQgis()

project = QgsProject.instance()
success = project.read(project_path)
if not success:
    raise ValueError(f"Failed to read project file: {project_path}")

layout = project.layoutManager().layoutByName(project_layout)
exporter = QgsLayoutExporter(layout)

if layout is None:
    raise RuntimeError(f"No layout found with name '{project_layout}'.")

map_item = None
for item in layout.items():
    if isinstance(item, QgsLayoutItemMap):
        map_item = item
        break

if map_item is None:
    raise RuntimeError(f"No QgsLayoutItemMap found in layout '{project_layout}'.")

metadata = []
map_crs = map_item.crs()

topo_sheet_layer = QgsProject.instance().mapLayersByName(topo_map_sheet)[0]

for feature in topo_sheet_layer.getFeatures():
    feature_code = str(feature["sheet_code"])
    # Skip if we're not listing all, and feature_code is not in the allowed list
    if not list_all_sheet_codes and feature_code not in sheet_codes:
        continue
    geom = feature.geometry()
    geom.transform(
        QgsCoordinateTransform(topo_sheet_layer.crs(), map_crs, QgsProject.instance())
    )
    bbox = geom.boundingBox()
    metadata.append(
        {
            "sheetCode": feature_code,
            "geometry": geom.asJson(),
            "epsg": map_crs.postgisSrid(),
            "bbox": [
                bbox.xMinimum(),
                bbox.yMinimum(),
                bbox.xMaximum(),
                bbox.yMaximum(),
            ],
        }
    )

json.dump(metadata, sys.stdout, ensure_ascii=False)
sys.stdout.write("\n")
sys.stdout.flush()

qgs.exitQgis()
