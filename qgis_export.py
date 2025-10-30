from qgis.core import *
import sys
import os

os.environ.update({
    'QGIS_USER_DB_FILE': '/tmp/qgis/qgis.db',
    'QGIS_AUTH_DB_DIR_PATH': '/tmp/qgis/',
    'QGIS_USER_DB_DIR_PATH': '/tmp/qgis/',
    'QGIS_CUSTOM_CONFIG_PATH': '/tmp/qgis/',
    'QT_QPA_PLATFORM': 'offscreen',
    'HOME': '/tmp/qgis/',  # Some QGIS components check HOME
    'XDG_CONFIG_HOME': '/tmp/qgis/',
    'XDG_DATA_HOME': '/tmp/qgis/',
    'XDG_CACHE_HOME': '/tmp/qgis/',
    'XDG_RUNTIME_DIR': '/tmp/qgis/' 
})

# Create necessary directories
os.makedirs('/tmp/qgis/', exist_ok=True)
os.makedirs('/tmp/qgis/.qgis3', exist_ok=True)
os.makedirs('/tmp/qgis/processing', exist_ok=True)

project_path = sys.argv[1]
center_x = float(sys.argv[2])
center_y = float(sys.argv[3])
dpi = int(sys.argv[4])

file_path = sys.argv[5]


QgsApplication.setPrefixPath("/usr", True)  # Adjust path as needed
qgs = QgsApplication([], False)  # False = no GUI
qgs.initQgis()

print(f"QGIS Version: {Qgis.QGIS_VERSION}")

project = QgsProject.instance()
project.read(project_path)

layers = project.mapLayers().values()

for layer in layers:
    if not layer.isValid():
           continue
    
    current_source = layer.source()
    
    #if not current_source.endswith(".gpkg"): 
         #continue
    print(f"Layer: {layer.name()}, Source: {current_source}")

layout = project.layoutManager().layoutByName("Topo50")
map_item = None
for item in layout.items():
    if isinstance(item, QgsLayoutItemMap):
        map_item = item
        break

page = layout.pageCollection().pages()[0]
page_width = page.pageSize().width()
page_height = page.pageSize().height()

# Get map item dimensions
map_width = map_item.rect().width()
map_height = map_item.rect().height()

# Calculate new position to center the map item on the page
new_x = (page_width - map_width) / 2
new_y = (page_height - map_height) / 2

current_extent = map_item.extent()
current_width = current_extent.width()
current_height = current_extent.height()

# Create a new extent centered on the given coordinates
new_center_point = QgsPointXY(center_x, center_y)
new_extent = QgsRectangle(
    new_center_point.x() - current_width / 2,
    new_center_point.y() - current_height / 2,
    new_center_point.x() + current_width / 2,
    new_center_point.y() + current_height / 2
)
map_item.setExtent(new_extent)

# Export the layout
exporter = QgsLayoutExporter(layout)


export_result = None
if file_path.endswith('.pdf'):
    pdf_settings = QgsLayoutExporter.PdfExportSettings()
    pdf_settings.dpi = dpi
    pdf_settings.rasterizeWholeImage = False
    export_result = exporter.exportToPdf(file_path, pdf_settings)
else:
    settings = QgsLayoutExporter.ImageExportSettings()
    settings.dpi = dpi  
    export_result = exporter.exportToImage(file_path, settings)

if export_result == QgsLayoutExporter.Success:
    print(f"Map exported successfully to: {file_path}")
else:
    print(f"Error exporting map: {exporter.errorMessage()}")

qgs.exitQgis()