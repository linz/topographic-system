from qgis.core import QgsApplication, QgsProject, QgsMapLayer
import sys
import os
import json

os.environ.update({"QT_QPA_PLATFORM": "offscreen"})

project_path = sys.argv[1]

# Init QGIS
QgsApplication.setPrefixPath("/usr", True)
qgs = QgsApplication([], False)
qgs.initQgis()

# Load project
project = QgsProject.instance()
if not project.read(project_path):
    raise RuntimeError(f"Failed to read project: {project_path}")

# Collect vector layer names
layer_names = [
    layer.source()
    for layer in project.mapLayers().values()
    if layer.type() == QgsMapLayer.VectorLayer
]

# Output JSON array
json.dump(layer_names, sys.stdout, ensure_ascii=False)
sys.stdout.write("\n")
sys.stdout.flush()

# Cleanup
qgs.exitQgis()
