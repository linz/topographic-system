import argparse
import glob
import json
import os
import sys
from dataclasses import dataclass

from qgis.core import (
    QgsApplication,
    QgsCoordinateTransform,
    QgsFeature,
    QgsLayoutExporter,
    QgsLayoutItemMap,
    QgsProject,
    QgsVectorLayer,
)
from qgis.PyQt.QtGui import QFontDatabase  # type: ignore[import-not-found]

os.environ.update({"QT_QPA_PLATFORM": "offscreen"})

UUID_MAP_ITEM = "{e1f230ea-4c9e-4ea9-a1f3-413ace3829fa}"

@dataclass
class ExportArgs:
    project_path: str
    file_output_path: str
    project_layout: str
    topo_map_sheet_name: str
    export_format: str
    dpi: int
    sheet_code: str
    excluded_layers: set


def parse_excluded_layer_names(excluded_layer_names: str) -> set:
    if excluded_layer_names is None:
        return set()
    try:
        excluded = json.loads(excluded_layer_names)
        if not isinstance(excluded, list):
            sys.stderr.write("Error: Excluded layer names must be a JSON array.\n")
            sys.exit(1)
        return set(str(item) for item in excluded)
    except json.JSONDecodeError as e:
        sys.stderr.write(f"Error parsing excluded_layer_names JSON: {e}\n")
        sys.exit(1)


def parse_args() -> ExportArgs:
    parser = argparse.ArgumentParser(description="Export QGIS data to files.")

    parser.add_argument(
        "--project", type=str, dest="project_path", required=True, help="Path to the QGIS project file."
    )
    parser.add_argument(
        "--output", type=str, dest="file_output_path", required=True, help="Directory path to save the output files."
    )
    parser.add_argument(
        "--layout", type=str, dest="project_layout", required=True, help="Name of the QGIS layout to export."
    )
    parser.add_argument(
        "--map-sheet-layer-name",
        type=str,
        dest="topo_map_sheet_name",
        required=True,
        help="Name of the layer that contains sheet polygons.",
    )
    parser.add_argument(
        "--format",
        type=str,
        dest="export_format",
        choices=["pdf", "tiff", "geotiff", "png"],
        required=True,
        help="Export format.",
    )
    parser.add_argument("--dpi", type=int, dest="dpi", required=True, help="DPI resolution for the export.")
    parser.add_argument("--sheet-code", type=str, dest="sheet_code", required=True, help="Sheet code identifier.")
    parser.add_argument(
        "--exclude-layers", type=str, dest="excluded_layer_names", help="JSON-encoded array of excluded layer names."
    )

    parsed = parser.parse_args()

    # Validate project path
    if not os.path.isfile(parsed.project_path):
        sys.stderr.write(f"Error: QGIS project file does not exist: {parsed.project_path}\n")
        sys.exit(1)

    # Validate DPI
    if parsed.dpi <= 0:
        sys.stderr.write(f"Error: DPI must be a positive integer, got: {parsed.dpi}\n")
        sys.exit(1)

    # Parse excluded layers JSON
    excluded_layers = parse_excluded_layer_names(parsed.excluded_layer_names)

    # Ensure output directory exists
    if not os.path.isdir(parsed.file_output_path):
        try:
            os.makedirs(parsed.file_output_path, exist_ok=True)
        except Exception as e:
            sys.stderr.write(f"Error: Output directory could not be created: {parsed.file_output_path}. Details: {e}\n")
            sys.exit(1)

    return ExportArgs(
        project_path=parsed.project_path,
        file_output_path=parsed.file_output_path,
        project_layout=parsed.project_layout,
        topo_map_sheet_name=parsed.topo_map_sheet_name,
        export_format=parsed.export_format,
        dpi=parsed.dpi,
        sheet_code=parsed.sheet_code,
        excluded_layers=excluded_layers,
    )


def find_sheet_feature(topo_sheet_layer: QgsVectorLayer, sheet_code: str) -> QgsFeature:
    fields = topo_sheet_layer.fields()
    if fields.indexFromName("sheet_code") == -1:
        raise KeyError(f"Layer '{topo_sheet_layer.name()}' is missing the 'sheet_code' attribute.")
    for feature in topo_sheet_layer.getFeatures():
        feature_code = str(feature["sheet_code"])
        if feature_code == sheet_code:
            return feature
    raise ValueError(f"Feature with sheet code '{sheet_code}' not found in layer '{topo_sheet_layer.name()}'")


def main():
    args = parse_args()

    QgsApplication.setPrefixPath("/usr", True)  # Adjust path as needed
    qgs = QgsApplication([], False)  # False = no GUI
    qgs.initQgis()

    try:
        # Load custom fonts
        project_dir = os.path.dirname(args.project_path)
        for font_file in glob.glob(os.path.join(project_dir, "*.otf")):
            QFontDatabase.addApplicationFont(font_file)

        project = QgsProject.instance()
        success = project.read(args.project_path)
        if not success:
            raise ValueError(f"Failed to read project file: {args.project_path}")

        layout = project.layoutManager().layoutByName(args.project_layout)
        if layout is None:
            raise ValueError(f"No layout found with name '{args.project_layout}'.")

        exporter = QgsLayoutExporter(layout)

        map_item = None
        for item in layout.items():
            if isinstance(item, QgsLayoutItemMap) and item.uuid() == UUID_MAP_ITEM:
                map_item = item
                break

        if map_item is None:
            raise ValueError(f"No QgsLayoutItemMap found in layout '{args.project_layout}'.")

        for layer in list(project.mapLayers().values()):
            if layer.name() in args.excluded_layers:
                project.removeMapLayer(layer.id())

        map_crs = map_item.crs()

        matching_layers = project.mapLayersByName(args.topo_map_sheet_name)
        if not matching_layers:
            raise ValueError(f"No layer found with name '{args.topo_map_sheet_name}'.")
        topo_sheet_layer = matching_layers[0]

        feature = find_sheet_feature(topo_sheet_layer, args.sheet_code)

        geom = feature.geometry()
        geom.transform(QgsCoordinateTransform(topo_sheet_layer.crs(), map_crs, project))
        bbox = geom.boundingBox()
        map_item.setExtent(bbox)

        export_result = None
        if args.export_format == "pdf":
            output_file = os.path.join(args.file_output_path, f"{args.sheet_code}.pdf")
            pdf_settings = QgsLayoutExporter.PdfExportSettings()
            pdf_settings.dpi = args.dpi
            pdf_settings.rasterizeWholeImage = False
            export_result = exporter.exportToPdf(output_file, pdf_settings)
        elif args.export_format in ["tiff", "geotiff", "png"]:
            ext = "tiff" if args.export_format in ["tiff", "geotiff"] else "png"
            output_file = os.path.join(args.file_output_path, f"{args.sheet_code}.{ext}")
            img_settings = QgsLayoutExporter.ImageExportSettings()
            img_settings.dpi = args.dpi
            if args.export_format == "geotiff":
                img_settings.exportMetadata = True  # Only for geotif
            export_result = exporter.exportToImage(output_file, img_settings)
        else:
            raise ValueError(f"Unsupported format: {args.export_format}")

        if export_result == QgsLayoutExporter.Success:
            print(output_file)
        else:
            raise ValueError(f"Error exporting map: {exporter.errorMessage()}")

    except Exception as e:
        sys.stderr.write(f"Error running qgis_export.py: {e}\n")
        sys.exit(1)
    finally:
        qgs.exitQgis()


if __name__ == "__main__":
    main()
