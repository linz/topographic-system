from qgis.core import (
    QgsApplication,
    QgsProject,
    QgsLayoutExporter,
)
import sys
import os
import json

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

if layout is None:
    raise RuntimeError(f"No layout found with name '{project_layout}'.")

exporter = QgsLayoutExporter(layout)

for layer in list(project.mapLayers().values()):
    if layer.name() in excluded_layer_names:
        project.removeMapLayer(layer.id())

atlas = layout.atlas()
atlas.setFilterFeatures(True)
atlas.setFilterExpression(f"\"sheet_code\" = '{sheet_code}'")
atlas.beginRender()
atlas.first()

export_result = None
if export_format == "pdf":
    output_file = os.path.join(file_output_path, f"{sheet_code}.pdf")
    pdf_settings = QgsLayoutExporter.PdfExportSettings()
    pdf_settings.dpi = dpi
    pdf_settings.rasterizeWholeImage = False
    export_result = exporter.exportToPdf(output_file, pdf_settings)
elif export_format in ["tiff", "geotiff", "png"]:
    ext = "tiff" if export_format in ["tiff", "geotiff"] else "png"
    output_file = os.path.join(file_output_path, f"{sheet_code}.{ext}")
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

atlas.endRender()
qgs.exitQgis()
