from qgis.core import *
import sys
import os

os.environ.update({'QT_QPA_PLATFORM': 'offscreen'})

project_path = sys.argv[1]
file_output_path = sys.argv[2]

QgsApplication.setPrefixPath("/usr", True)  # Adjust path as needed
qgs = QgsApplication([], False)  # False = no GUI
qgs.initQgis()

project = QgsProject.instance()
project.read(project_path)

layout = project.layoutManager().layoutByName("Topo50")

pdf_settings = QgsLayoutExporter.PdfExportSettings()
pdf_settings.dpi = 300
pdf_settings.rasterizeWholeImage = False
exporter = QgsLayoutExporter(layout)

map_item = None
for item in layout.items():
    if isinstance(item, QgsLayoutItemMap):
        map_item = item
        break

topo_sheet_layer = QgsProject.instance().mapLayersByName("nz_topo_map_sheet")[0]
for feature in topo_sheet_layer.getFeatures():
    geom = feature.geometry()
    geom.transform(QgsCoordinateTransform(topo_sheet_layer.crs(), map_item.crs(), QgsProject.instance()))
    map_item.setExtent(geom.boundingBox())
    exporter.exportToPdf(os.path.join(file_output_path, feature["sheet_code"] + ".pdf"), pdf_settings)

qgs.exitQgis()