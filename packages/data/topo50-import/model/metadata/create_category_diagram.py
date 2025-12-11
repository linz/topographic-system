import os
import json
from graphviz import Digraph # type: ignore
import subprocess


def render(output_folder, cat, dot, summary=""):
    filename = os.path.join(
        output_folder, f"{cat.replace(' ', '_')}{summary}_diagram.dot"
    )
    filename = filename.replace("&", "and")

    dot.save(filename)

    svg_filename = filename.replace(".dot", ".svg")
    gif_filename = filename.replace(".dot", ".png")
    subprocess.run(["dot", "-Tsvg", filename, "-o", svg_filename], check=True)
    print(f"Generated SVG: {svg_filename}")
    print(f"Generated PNG: {gif_filename}")


def create_main_and_layer_diagrams(metadata_file_path, output_folder):
    # Load metadata
    with open(metadata_file_path, "r") as f:
        metadata = json.load(f)

    # Create output folder
    os.makedirs(output_folder, exist_ok=True)

    # Define colors for categories
    category_colors = {
        "Transport and Infrastructure": "lightblue",
        "Buildings & Structures": "lightgreen",
        "Land Cover & Land Use": "orange",
        "Water Marine Coastal Hydrographic": "lightcyan",
        "Terrain": "yellow",
        "Administrative & Reference": "pink",
    }

    # Group layers by category
    categories = {}
    for layer_name, layer_info in metadata.items():
        cat = layer_info.get("layer_category", "Unknown")
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(
            (
                layer_name,
                layer_info.get("feature_types", {}),
                layer_info.get("features", {}),
            )
        )

    # Generate one diagram per category
    for cat, layers in categories.items():
        dot = Digraph(comment=f"{cat} Hierarchy", format="png")
        dot.attr(rankdir="LR", size="8")
        dot.node(
            cat,
            cat,
            shape="box",
            style="filled",
            color=category_colors.get(cat, "white"),
        )

        dot_detailed = Digraph(comment=f"{cat} Hierarchy", format="png")
        dot_detailed.attr(rankdir="LR", size="8")
        dot_detailed.node(
            cat,
            cat,
            shape="box",
            style="filled",
            color=category_colors.get(cat, "white"),
        )

        for layer_name, feature_types, features in layers:
            themes = set()
            for feature_info in features.items():
                theme = feature_info[1].get("theme", "Unknown")
                themes.add(theme)
            themes_str = ", ".join(sorted(themes))
            dot.node(
                layer_name,
                layer_name,
                shape="box",
                style="filled",
                fillcolor="lightblue",
            )
            dot.edge(cat, layer_name)
            dot.edge(layer_name, f"{layer_name}_themes")
            dot.node(
                f"{layer_name}_themes",
                themes_str,
                shape="box",
                style="filled",
                fillcolor="lightgray",
            )

            dot_detailed.node(
                layer_name,
                layer_name,
                shape="box",
                style="filled",
                fillcolor="lightblue",
            )
            dot_detailed.edge(cat, layer_name)
            dot_detailed.edge(layer_name, f"{layer_name}_themes")
            dot_detailed.node(
                f"{layer_name}_themes",
                themes_str,
                shape="box",
                style="filled",
                fillcolor="lightgray",
            )
            feature_types_str = (
                ", ".join(feature_types)
                if isinstance(feature_types, list)
                else str(feature_types)
            )
            dot_detailed.node(
                f"{layer_name}_feature_types",
                feature_types_str,
                shape="box",
                style="filled",
                fillcolor="lightyellow",
            )
            dot_detailed.edge(f"{layer_name}_themes", f"{layer_name}_feature_types")

        render(output_folder, cat, dot, "_summary")
        render(output_folder, cat, dot_detailed)

        # dot.render(filename, view=False)

    print("dot files generated in 'category_diagrams' folder.")


if __name__ == "__main__":
    metadata_folder = r"C:\Data\Model\metadata"
    metadata_source_folder = r"C:\Data\Model\metadata_source"
    metadata_file = os.path.join(metadata_folder, "metadata_info.json")
    output_folder = os.path.join(metadata_source_folder, "model-themes")
    os.makedirs(output_folder, exist_ok=True)
    create_main_and_layer_diagrams(metadata_file, output_folder)
