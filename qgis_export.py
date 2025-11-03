from qgis.core import *
import sys
import os

os.environ.update({'QT_QPA_PLATFORM': 'offscreen'})

project_path = sys.argv[1]
x_min = float(sys.argv[2])
y_min = float(sys.argv[3])
x_max = float(sys.argv[4])
y_max = float(sys.argv[5])
dpi = int(sys.argv[6])
file_output_path = sys.argv[7]


QgsApplication.setPrefixPath("/usr", True)  # Adjust path as needed
qgs = QgsApplication([], False)  # False = no GUI
qgs.initQgis()

project = QgsProject.instance()
project.read(project_path)

layout = project.layoutManager().layoutByName("Topo50")
map_item = None
for item in layout.items():
    if isinstance(item, QgsLayoutItemMap):
        map_item = item
        break

new_extent = QgsRectangle(x_min, y_min, x_max, y_max)
map_item.setExtent(new_extent)

exporter = QgsLayoutExporter(layout)
export_result = None
if file_output_path.endswith('.pdf'):
    pdf_settings = QgsLayoutExporter.PdfExportSettings()
    pdf_settings.dpi = dpi
    pdf_settings.rasterizeWholeImage = False
    export_result = exporter.exportToPdf(file_output_path, pdf_settings)
else:
    settings = QgsLayoutExporter.ImageExportSettings()
    settings.dpi = dpi  
    export_result = exporter.exportToImage(file_output_path, settings)

if export_result == QgsLayoutExporter.Success:
    print(f"Map exported successfully to: {file_output_path}")
else:
    print(f"Error exporting map: {exporter.errorMessage()}")

qgs.exitQgis()