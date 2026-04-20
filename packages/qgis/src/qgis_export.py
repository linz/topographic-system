from qgis.core import (
    QgsApplication,
    QgsCoordinateTransform,
    QgsExpression,
    QgsExpressionContext,
    QgsExpressionContextScope,
    QgsFeature,
    QgsLayoutExporter,
    QgsLayoutItemMap,
    QgsProject,
)

import json
import os
import sys

from datetime import datetime

os.environ.update({"QT_QPA_PLATFORM": "offscreen"})

project_path = sys.argv[1]
file_output_path = sys.argv[2]
project_layout = sys.argv[3]
topo_map_sheet = sys.argv[4]
export_format = sys.argv[5]
dpi = int(sys.argv[6])
sheet_code = sys.argv[7]
excluded_layer_names = set(json.loads(sys.argv[8]))

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

# find map_item
map_item = None
for item in layout.items():
    if isinstance(item, QgsLayoutItemMap):
        map_item = item
        break

if map_item is None:
    raise RuntimeError(f"No QgsLayoutItemMap found in layout '{project_layout}'.")

for layer in list(project.mapLayers().values()):
    if layer.name() in excluded_layer_names:
        project.removeMapLayer(layer.id())

map_crs = map_item.crs()

# find topo_sheet_feature
topo_sheet_layer = QgsProject.instance().mapLayersByName(topo_map_sheet)[0]

topo_sheet_feature = None
for feature in topo_sheet_layer.getFeatures():
    if not isinstance(feature, QgsFeature):
        raise TypeError("feature is not a QgsFeature")

    feature_sheet_code = feature.attribute("sheet_code")
    if not isinstance(feature_sheet_code, str):
        raise TypeError("feature['sheet_code'] is not a str")

    if feature_sheet_code == sheet_code:
        topo_sheet_feature = feature
        break

if topo_sheet_feature is None:
    raise RuntimeError(f"No QgsFeature found with sheet_code '{sheet_code}'.")

# consume topo_sheet_feature
geom = feature.geometry()
geom.transform(
    QgsCoordinateTransform(topo_sheet_layer.crs(), map_crs, QgsProject.instance())
)
bbox = geom.boundingBox()
map_item.setExtent(bbox)

# calculate map_sheet center (lat, lon)
center = bbox.center()
center_lat = center.y()
center_lon = center.x()

# calculate variables
scope = QgsExpressionContextScope()
scope.setVariable("model_name", "igrf13")
scope.setVariable("date", datetime.today())
scope.setVariable("centre_lat", center_lat)
scope.setVariable("centre_lon", center_lon)
scope.setVariable("height", 0)
scope.setVariable("model_path", "/app/qgis/assets/models/")

context = QgsExpressionContext()
context.appendScope(scope)

expr = QgsExpression(
    "magnetic_declination(@model_name, @date, @centre_lat, @centre_lon, @height, @model_path)"
)
if expr.hasParserError():
    raise RuntimeError(expr.parserErrorString())

declination = expr.evaluate(context)
if expr.hasEvalError():
    raise RuntimeError(expr.evalErrorString())
if not isinstance(declination, float):
    raise TypeError("The calculated magnetic declination value is not a float number.")

# round variables
project_centre_latitude = round(center_lat, 4)
project_centre_longitude = round(center_lon, 4)
project_declination = round(declination, 1)
project_gm_angle = round(declination * 2) / 2
project_mills = round(project_gm_angle * 17.7778)

# append variables to the project
project.setCustomVariables(
    {
        **project.customVariables(),
        "project_map_sheet_code": sheet_code,
        "project_centre_latitude": project_centre_latitude,
        "project_centre_longitude": project_centre_longitude,
        # project_convergence
        "project__declination": project_declination,
        "project_gm_angle": project_gm_angle,
        "project_mills": project_mills,
        # project_years_for_pos_half_deg
    }
)

print(
    sheet_code,
    project_centre_latitude,
    project_centre_longitude,
    project_declination,
    project_gm_angle,
    project_mills,
)

# export map
export_result = None
if export_format == "pdf":
    output_file = os.path.join(file_output_path, f"{feature_sheet_code}.pdf")
    pdf_settings = QgsLayoutExporter.PdfExportSettings()
    pdf_settings.dpi = dpi
    pdf_settings.rasterizeWholeImage = False
    export_result = exporter.exportToPdf(output_file, pdf_settings)
elif export_format in ["tiff", "geotiff", "png"]:
    ext = "tiff" if export_format in ["tiff", "geotiff"] else "png"
    output_file = os.path.join(file_output_path, f"{feature_sheet_code}.{ext}")
    img_settings = QgsLayoutExporter.ImageExportSettings()
    img_settings.dpi = dpi
    if export_format == "geotiff":
        img_settings.exportMetadata = True  # Only for geotif
    export_result = exporter.exportToImage(output_file, img_settings)
else:
    raise ValueError(f"Unsupported format: {export_format}")

if export_result == QgsLayoutExporter.Success:
    print(output_file)
else:
    print(f"Error exporting map: {exporter.errorMessage()}")

qgs.exitQgis()
