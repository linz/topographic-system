from qgis.core import (
    QgsCoordinateTransform,
    QgsExpression,
    QgsExpressionContextUtils,
    QgsFeature,
    QgsFeatureRequest,
    QgsLayoutItemMap,
    QgsPrintLayout,
    QgsProject,
    QgsVectorLayer,
)


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

    example_text = carto_match["text_string"]
    if example_text is None:
        raise RuntimeError(
            f"Example point with example_point_id = {example_point_id} in {carto_text_layer_name} "
            "has a null text_string."
        )

    # strip new lines
    example_text = example_text.replace("\n", " ")
    # strip spaces between /
    example_text = example_text.replace(" / ", "/")

    example_geom = example_feature.geometry()
    example_geom.transform(QgsCoordinateTransform(example_layer.crs(), map_main.crs(), project))
    QgsExpressionContextUtils.setLayoutVariable(layout, "example_x", example_geom.asPoint().x())
    QgsExpressionContextUtils.setLayoutVariable(layout, "example_y", example_geom.asPoint().y())
    QgsExpressionContextUtils.setLayoutVariable(layout, "example_class", example_format.format(example_text))
