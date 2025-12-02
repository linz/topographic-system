from qgis.core import (
    QgsApplication,
    QgsProject,
    QgsLayoutExporter,
    QgsCoordinateTransform,
    QgsLayoutItemMap
)
import sys
import os
import json

os.environ.update({'QT_QPA_PLATFORM': 'offscreen'})

project_path = sys.argv[1]
file_output_path = sys.argv[2]
export_format = sys.argv[3]
dpi = int(sys.argv[4])
sheet_codes = sys.argv[5:]

QgsApplication.setPrefixPath("/usr", True)  # Adjust path as needed
qgs = QgsApplication([], False)  # False = no GUI
qgs.initQgis()

project = QgsProject.instance()
success = project.read(project_path)
if not success:
    raise ValueError(f"Failed to read project file: {project_path}")

layout = project.layoutManager().layoutByName("Topo50")
exporter = QgsLayoutExporter(layout)

map_item = None
for item in layout.items():
    if isinstance(item, QgsLayoutItemMap):
        map_item = item
        break

metadata = []
map_crs = map_item.crs()

topo_sheet_layer = QgsProject.instance().mapLayersByName("nz_topo_map_sheet")[0]
for feature in topo_sheet_layer.getFeatures():
    feature_code = str(feature["sheet_code"])
    # skip if this sheet_code is not in the list passed from CLI
    if feature_code not in sheet_codes:
        continue
    geom = feature.geometry()
    geom.transform(QgsCoordinateTransform(topo_sheet_layer.crs(), map_crs, QgsProject.instance()))
    bbox = geom.boundingBox()
    map_item.setExtent(bbox)

    export_result = None
    if export_format == "pdf":
        output_file = os.path.join(file_output_path, f"{feature_code}.pdf")
        pdf_settings = QgsLayoutExporter.PdfExportSettings()
        pdf_settings.dpi = dpi
        pdf_settings.rasterizeWholeImage = False
        export_result = exporter.exportToPdf(output_file, pdf_settings)
    elif export_format in ["tiff", "geotiff"]:
        output_file = os.path.join(file_output_path, f"{feature_code}.tiff")
        img_settings = QgsLayoutExporter.ImageExportSettings()
        img_settings.dpi = dpi
        if export_format == "geotiff":
            img_settings.exportMetadata = True  # Only for geotif
        export_result = exporter.exportToImage(output_file, img_settings)
    else:
        raise ValueError(f"Unsupported format: {export_format}")

    if export_result == QgsLayoutExporter.Success:
        metadata.append(
            {
                "sheetCode": feature_code,
                "geometry": geom.asJson(),
                "epsg": map_crs.postgisSrid(),
                "bbox": [bbox.xMinimum(), bbox.yMinimum(), bbox.xMaximum(), bbox.yMaximum()],
            }
        )
    else:
        print(f"Error exporting map: {exporter.errorMessage()}")

json.dump(metadata, sys.stdout, ensure_ascii=False)
sys.stdout.write("\n")
sys.stdout.flush()

qgs.exitQgis()