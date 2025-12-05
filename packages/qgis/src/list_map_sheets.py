from qgis.core import (
    QgsApplication,
    QgsProject
)
import sys
import os
import json

os.environ.update({'QT_QPA_PLATFORM': 'offscreen'})

project_path = sys.argv[1]
layer_name = sys.argv[2]

# Init QGIS
QgsApplication.setPrefixPath("/usr", True)
qgs = QgsApplication([], False)
qgs.initQgis()

# Load project
project = QgsProject.instance()
success = project.read(project_path)
if not success:
    raise ValueError(f"Failed to read project file: {project_path}")

# Get topo sheet layer
layers = QgsProject.instance().mapLayersByName(layer_name)
if not layers:
    raise ValueError(f"Layer '{layer_name}' not found in project")
topo_sheet_layer = layers[0]

# Collect all sheet codes
map_sheets = []
for feature in topo_sheet_layer.getFeatures():
    feature_code = str(feature["sheet_code"])
    map_sheets.append(feature_code)

# Output as JSON array (for Argo withParam / fan-out)
json.dump(map_sheets, sys.stdout, ensure_ascii=False)
sys.stdout.write("\n")
sys.stdout.flush()

# Cleanup
qgs.exitQgis()
