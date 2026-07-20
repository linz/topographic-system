import argparse
import glob
import json
import os
import sys
from dataclasses import dataclass

from qgis.core import (
    QgsApplication,
    QgsCoordinateTransform,
    QgsExpression,
    QgsExpressionContextUtils,
    QgsFeature,
    QgsFeatureRequest,
    QgsLayoutExporter,
    QgsLayoutItemMap,
    QgsPrintLayout,
    QgsProject,
    QgsVectorLayer,
)
from qgis.PyQt.QtGui import QFontDatabase  # type: ignore[import-not-found]
from utils.calculate_mag_info import calculate_mag_info
from utils.render_mag_info import render_mag_info

os.environ.update({"QT_QPA_PLATFORM": "offscreen"})


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
    carto_text_layer_name: str


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
    parser.add_argument(
        "--carto-text-layer-name",
        type=str,
        dest="carto_text_layer_name",
        help="Name of the carto text layer that supplies example point labels.",
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
        carto_text_layer_name=parsed.carto_text_layer_name,
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


def find_feature_by_field(layer: QgsVectorLayer, field: str, value: str) -> QgsFeature | None:
    matches = layer.getFeatures(
        QgsFeatureRequest().setFilterExpression(QgsExpression.createFieldEqualityExpression(field, value))
    )
    match = next(matches, None)
    matches.close()
    return match


def find_example_point(project: QgsProject, example_point_id: str) -> tuple[QgsVectorLayer, QgsFeature, str]:
    """The id references a feature in either the trig_point or geographic_name layer."""
    example_label_formats = {
        "trig_point": "▲ {}",
        "geographic_name": "<i>· {}</i>",
    }
    for layer_name, label_format in example_label_formats.items():
        layers = project.mapLayersByName(layer_name)
        if not layers:
            continue
        match = find_feature_by_field(layers[0], "id", example_point_id)
        if match is not None:
            return layers[0], match, label_format
    raise RuntimeError(f"No feature with id = {example_point_id} found in trig_point or geographic_name.")


def set_example_grid_reference(
    layout: QgsPrintLayout,
    feature: QgsFeature,
    map_main: QgsLayoutItemMap,
    project: QgsProject,
    carto_text_layer_name: str,
) -> None:
    if "example_point_id" not in feature.fields().names() or feature["example_point_id"] is None:
        raise ValueError("Map sheet feature is missing an example_point_id.")

    example_point_id = feature["example_point_id"]

    # example_point_id references a feature in trig_point or geographic_name
    example_layer, example_feature, example_format = find_example_point(project, example_point_id)

    # The rendered label text comes from the carto text layer
    carto_text_layers = project.mapLayersByName(carto_text_layer_name)
    if not carto_text_layers:
        raise RuntimeError(f"No layer found with name '{carto_text_layer_name}'.")
    carto_match = find_feature_by_field(carto_text_layers[0], "example_point_id", example_point_id)
    if carto_match is None:
        raise RuntimeError(f"No feature with example_point_id = {example_point_id} found in {carto_text_layer_name}.")

    example_geom = example_feature.geometry()
    example_geom.transform(QgsCoordinateTransform(example_layer.crs(), map_main.crs(), project))
    QgsExpressionContextUtils.setLayoutVariable(layout, "example_x", example_geom.asPoint().x())
    QgsExpressionContextUtils.setLayoutVariable(layout, "example_y", example_geom.asPoint().y())
    QgsExpressionContextUtils.setLayoutVariable(
        layout, "example_class", example_format.format(carto_match["text_string"])
    )


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

        # Load project
        project = QgsProject.instance()
        success = project.read(args.project_path)
        if not success:
            raise ValueError(f"Failed to read project file: {args.project_path}")

        # Load layout
        layout = project.layoutManager().layoutByName(args.project_layout)
        if not isinstance(layout, QgsPrintLayout):
            raise ValueError(f"No layout found with name '{args.project_layout}'.")

        # Load map items
        map_main = layout.itemById("map_main")
        if not isinstance(map_main, QgsLayoutItemMap):
            raise ValueError(f"No 'map_main' QgsLayoutItemMap item found in layout '{args.project_layout}'.")

        # Remove layers
        for layer in list(project.mapLayers().values()):
            if layer.name() in args.excluded_layers:
                project.removeMapLayer(layer.id())

        # Find map sheet feature
        matching_layers = project.mapLayersByName(args.topo_map_sheet_name)
        if not matching_layers:
            raise ValueError(f"No layer found with name '{args.topo_map_sheet_name}'.")
        topo_sheet_layer = matching_layers[0]
        feature = find_sheet_feature(topo_sheet_layer, args.sheet_code)

        # Set map item extents
        geom = feature.geometry()
        geom.transform(QgsCoordinateTransform(topo_sheet_layer.crs(), map_main.crs(), project))
        bbox = geom.boundingBox()
        map_main.setExtent(bbox)

        # Handle magnetic info
        mag_info_raw = calculate_mag_info(project, feature, topo_sheet_layer.crs())
        mag_info_render = render_mag_info(mag_info_raw)

        for key, value in mag_info_render.items():
            QgsExpressionContextUtils.setLayoutVariable(layout, key, value)

        QgsExpressionContextUtils.setLayoutVariable(layout, "sheet_code", args.sheet_code)

        set_example_grid_reference(layout, feature, map_main, project, args.carto_text_layer_name)

        # Generate outputs
        exporter = QgsLayoutExporter(layout)

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
