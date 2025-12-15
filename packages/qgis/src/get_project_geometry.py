from qgis.core import QgsApplication, QgsProject, QgsVectorLayer, QgsGeometry
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
success = project.read(project_path)
if not success:
    raise ValueError(f"Failed to read project file: {project_path}")

all_geometries = []

# Collect all geometries from all vector layers
for layer in project.mapLayers().values():
    if isinstance(layer, QgsVectorLayer) and layer.geometryType() != -1:
        for feature in layer.getFeatures():
            geom = feature.geometry()
            if geom is not None:
                all_geometries.append(geom)

# Compute union of all geometries
if all_geometries:
    union_geom = QgsGeometry.unaryUnion(all_geometries)
    bbox = union_geom.boundingBox()
    union_wkt = union_geom.asWkt()
else:
    union_geom = None
    bbox = None
    union_wkt = None

# Prepare output
output = {
    "geometry": union_wkt,
    "bbox": [
        bbox.xMinimum(),
        bbox.yMinimum(),
        bbox.xMaximum(),
        bbox.yMaximum()
    ] if bbox else None
}

# Output as JSON
json.dump(output, sys.stdout, ensure_ascii=False)
sys.stdout.write("\n")
sys.stdout.flush()

# Cleanup
qgs.exitQgis()
