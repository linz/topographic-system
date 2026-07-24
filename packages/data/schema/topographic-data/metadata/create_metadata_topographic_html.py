import os
import shutil
import json


def ensure_output_folders(output_folder):
    images_folder = os.path.join(output_folder, "images")
    model_images_folder = os.path.join(images_folder, "model")
    feature_images_folder = os.path.join(images_folder, "features")
    aerial_images_folder = os.path.join(images_folder, "aerial")

    os.makedirs(model_images_folder, exist_ok=True)
    os.makedirs(feature_images_folder, exist_ok=True)
    os.makedirs(aerial_images_folder, exist_ok=True)

    return {
        "images": images_folder,
        "model": model_images_folder,
        "features": feature_images_folder,
        "aerial": aerial_images_folder,
    }


def copy_asset_if_present(source_path, destination_folder):
    if not os.path.exists(source_path):
        return None

    filename = os.path.basename(source_path)
    shutil.copy2(source_path, os.path.join(destination_folder, filename))
    return filename


def create_markdown_file(table_dictionary, output_folder):
    image_folders = ensure_output_folders(output_folder)
    summary_data = table_dictionary.get("summary", {})
    category_items = [
        (category_name, tables)
        for category_name, tables in table_dictionary.items()
        if category_name != "summary"
    ]

    markdown_lines = ["# Topo50 Features Documentation", ""]

    for category_name, tables in category_items:
        markdown_lines.append(f"## {category_name}")
        markdown_lines.append("")
        markdown_lines.append(
            f"**Features in this table:** {', '.join(tables['feature_types'])}"
        )
        markdown_lines.append("")
        markdown_lines.append(
            f"**Total feature types:** {len(tables['feature_types'])}"
        )
        markdown_lines.append("")
        markdown_lines.append(f"**Layer Category:** {tables['layer_category']}")
        markdown_lines.append("")
        markdown_lines.append(
            f"**Layer Description:** {tables['layer_description']}"
        )
        markdown_lines.append("")

        model_filename = copy_asset_if_present(
            tables["model_picture_path"], image_folders["model"]
        )
        if model_filename:
            markdown_lines.append("### Model Schema")
            markdown_lines.append("")
            markdown_lines.append(
                f"![Model for {category_name}](images/model/{model_filename})"
            )
            markdown_lines.append("")

        for feature_type, feature_info in tables.get("features", {}).items():
            markdown_lines.append(f"### {feature_type}")
            markdown_lines.append("")
            markdown_lines.append(f"**Theme:** {feature_info['theme']}")
            markdown_lines.append("")
            markdown_lines.append(f"**LDS Table:** {feature_info['table_lds']}")
            markdown_lines.append("")
            markdown_lines.append(f"**Type:** {feature_info['table_type']}")
            markdown_lines.append("")
            markdown_lines.append(
                f"**Description:** {feature_info['description']}"
            )
            markdown_lines.append("")

            feature_filename = copy_asset_if_present(
                feature_info["feature_picture_path"], image_folders["features"]
            )
            if feature_filename:
                markdown_lines.append("#### Feature Visualization")
                markdown_lines.append("")
                markdown_lines.append(
                    f"![Feature {feature_type}](images/features/{feature_filename})"
                )
                markdown_lines.append("")

            aerial_filename = copy_asset_if_present(
                feature_info["aerial_picture_path"], image_folders["aerial"]
            )
            if aerial_filename:
                markdown_lines.append("#### Aerial View")
                markdown_lines.append("")
                markdown_lines.append(
                    f"![Aerial {feature_type}](images/aerial/{aerial_filename})"
                )
                markdown_lines.append("")

    markdown_file_path = os.path.join(
        output_folder, "topographic_layers_metadata.md"
    )
    with open(markdown_file_path, "w", encoding="utf-8") as markdown_file:
        markdown_file.write("\n".join(markdown_lines).rstrip() + "\n")

    index_lines = [
        "# Topographic Features Documentation",
        "",
        "## Overview",
        "",
        f"**Total Tables:** {len(category_items)}",
        "",
        "**Total Feature Types:** "
        + str(sum(len(table_info["feature_types"]) for _, table_info in category_items)),
        "",
        "## Documentation",
        "",
        "[View Complete Documentation](topographic_layers_metadata.md)",
        "",
    ]

    if summary_data:
        index_lines.extend(["## Category Diagrams", ""])
        for category_name, summary_info in summary_data.items():
            model_filename = copy_asset_if_present(
                summary_info[2], image_folders["model"]
            )
            if model_filename:
                heading = category_name.replace("_", " ").title()
                index_lines.append(
                    f"### {heading} Layers -> Themes -> Feature Types"
                )
                index_lines.append("")
                index_lines.append(
                    f"![Model for {category_name}](images/model/{model_filename})"
                )
                index_lines.append("")

    index_lines.extend(["## Layer Index", ""])
    for category_name, tables in category_items:
        feature_types = ", ".join(tables["feature_types"])
        index_lines.append(
            f"- [{category_name}](topographic_layers_metadata.md#{category_name})"
        )
        index_lines.append(
            f"  - {len(tables['feature_types'])} feature types: {feature_types}"
        )

    index_markdown_path = os.path.join(output_folder, "index.md")
    with open(index_markdown_path, "w", encoding="utf-8") as index_file:
        index_file.write("\n".join(index_lines).rstrip() + "\n")

    print(f"Markdown index created: {index_markdown_path}")
    print(f"Markdown documentation created: {markdown_file_path}")
    return markdown_file_path

def create_html_file(table_dictionary, html_output_folder):
    """
    Create a static HTML file with all feature information and copy images for self-contained package.
    """

    image_folders = ensure_output_folders(html_output_folder)
    model_images_folder = image_folders["model"]
    feature_images_folder = image_folders["features"]
    aerial_images_folder = image_folders["aerial"]

    # Start HTML content
    html_content = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
        model_filename = copy_asset_if_present(
            tables["model_picture_path"], model_images_folder
        )
        if model_filename:
                .table-title { background-color: #f5f5f5; padding: 10px; margin: -20px -20px 20px -20px; }
                .feature { margin-bottom: 20px; padding: 15px; border-left: 3px solid #007acc; }
                .feature-images { display: flex; gap: 20px; margin-top: 10px; }
                .feature-images img { max-width: 300px; height: auto; border: 1px solid #ccc; }
                .theme { color: #007acc; font-weight: bold; }
            </style>
        </head>
        <body>
            <h1>Topo50 Features Documentation</h1>
        """

    # Process each table
    for category_name, tables in table_dictionary.items():
        if category_name == "summary":
            continue
        html_content += '<div class="table-section">\n'
        html_content += f'<div class="table-title"><h2 id="{category_name}">{category_name}</h2></div>\n'
        # Add summary of feature types in this table
        feature_types = tables["feature_types"]
        html_content += f"<p><strong>Features in this table:</strong> {', '.join(feature_types)}</p>\n"
        html_content += (
            f"<p><strong>Total feature types:</strong> {len(feature_types)}</p>\n"
        )
        html_content += (
            f"<p><strong>Layer Category:</strong> {tables['layer_category']}</p>\n"
        )
        html_content += f"<p><strong>Layer Description:</strong> {tables['layer_description']}</p>\n"
        html_content += '<div class="model-images">\n'
        # Copy and reference model picture
        model_src = tables["model_picture_path"]
        if os.path.exists(model_src):
            model_filename = os.path.basename(model_src)
            model_dest = os.path.join(model_images_folder, model_filename)
            shutil.copy2(model_src, model_dest)
            html_content += f'<div><h4>Model Schema</h4><img src="images/model/{model_filename}" alt="Model for {category_name}" style="max-width: 350px; height: 250px;"></div>\n'
        html_content += "</div>\n"

        features = tables.get("features", {})

        for feature_type, feature_info in features.items():
            html_content += '<div class="feature">\n'
            html_content += f"<h3>{feature_type}</h3>\n"
            html_content += (
                f'<p><span class="theme">Theme:</span> {feature_info["theme"]}</p>\n'
            )
            html_content += f'<p><span class="theme">LDS Table:</span> {feature_info["table_lds"]}</p>\n'
            html_content += f'<p><span class="theme">Type:</span> {feature_info["table_type"]}</p>\n'
            html_content += f'<p><span class="theme">Description:</span> {feature_info["description"]}</p>\n'

            html_content += '<div class="feature-images">\n'

            # Copy and reference feature picture
            feature_filename = copy_asset_if_present(
                feature_info["feature_picture_path"], feature_images_folder
            )
            aerial_filename = copy_asset_if_present(
                feature_info["aerial_picture_path"], aerial_images_folder
            )
            if feature_filename:
                html_content += f'<div><h4>Feature Visualization</h4><img src="images/features/{feature_filename}" alt="Feature {feature_type}" style="border: none;"></div>\n'
            # Copy and reference aerial picture
            if aerial_filename:
                html_content += f'<div><h4>Aerial View</h4><img src="images/aerial/{aerial_filename}" alt="Aerial {feature_type}" style="border: none;"></div>\n'
            html_content += "</div>\n"
            html_content += "</div>\n"

        html_content += "</div>\n"
    html_content += """
        </body>
        </html>
        """

    # Write HTML file
    html_file_path = os.path.join(
        html_output_folder, "topographic_layers_metadata.html"
    )
    with open(html_file_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    # Create an index page with table of contents
    summary_data = table_dictionary.get("summary", {})
    category_items = [
        (category_name, table_info)
        for category_name, table_info in table_dictionary.items()
        if category_name != "summary"
    ]

    index_html_content = (
        """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Topographic Data Layers - Index</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                .index-section { margin-bottom: 30px; padding: 20px; border: 1px solid #ddd; }
                .index-title { background-color: #f5f5f5; padding: 15px; margin: -20px -20px 20px -20px; }
                .table-list { list-style-type: none; padding: 0; }
                .table-item { margin: 10px 0; padding: 10px; border-left: 3px solid #007acc; }
                .table-item a { text-decoration: none; color: #007acc; font-weight: bold; }
                .feature-count { color: #666; font-size: 0.9em; }
                .stats { background-color: #f9f9f9; padding: 15px; border-radius: 5px; margin-bottom: 20px; }
            </style>
        </head>
        <body>
            <h1>Topographic Features Documentation</h1>
            
            <div class="stats">
                <h3>Overview</h3>
                <p><strong>Total Tables:</strong> """
        + str(len(category_items))
        + """</p>
                <p><strong>Total Feature Types:</strong> """
        + str(
            sum(
                len(table_info["feature_types"])
                for _, table_info in category_items
            )
        )
        + """</p>
            </div>

                        
            <div class="index-section">
                <div class="index-title"><h2>Documentation</h2></div>
                <p><a href="topographic_layers_metadata.html">View Complete Documentation</a></p>
            </div>
            
            <div class="index-section">
                <div class="index-title"><h2>Layer Index</h2></div>
                <ul class="table-list">
        """
    )

    for category_name, summary_info in summary_data.items():
        index_html_content += '<div class="model-drawings">\n'
        model_filename = copy_asset_if_present(summary_info[2], model_images_folder)

        if model_filename:

            if category_name == "Buildings_and_Structures":
                index_html_content += f'<div><h4>{category_name.replace("_", " ").title()} Layers -> Themes -> Feature Types </h4><img src="images/model/{model_filename}" alt="Model for {category_name}" style="width: auto; height: 500px;"></div>\n'
            elif category_name == "Administrative_and_Reference":
                index_html_content += f'<div><h4>{category_name.replace("_", " ").title()} Layers -> Themes -> Feature Types </h4><img src="images/model/{model_filename}" alt="Model for {category_name}" style="width: auto; height: 100px;"></div>\n'
            elif category_name == "Land_Cover_and_Land_Use":
                index_html_content += f'<div><h4>{category_name.replace("_", " ").title()} Layers -> Themes -> Feature Types </h4><img src="images/model/{model_filename}" alt="Model for {category_name}" style="width: auto; height: 700px;"></div>\n'
            elif category_name == "Relief_and_Terrain":
                index_html_content += f'<div><h4>{category_name.replace("_", " ").title()} Layers -> Themes -> Feature Types </h4><img src="images/model/{model_filename}" alt="Model for {category_name}" style="width: auto; height: 250px;"></div>\n'
            elif category_name == "Transport_and_Infrastructure":
                index_html_content += f'<div><h4>{category_name.replace("_", " ").title()} Layers -> Themes -> Feature Types </h4><img src="images/model/{model_filename}" alt="Model for {category_name}" style="width: auto; height: 600px;"></div>\n'
            elif category_name == "Water_and_Marine":
                index_html_content += f'<div><h4>{category_name.replace("_", " ").title()} Layers -> Themes -> Feature Types </h4><img src="images/model/{model_filename}" alt="Model for {category_name}" style="width: auto; height: 400px;"></div>\n'
            else:
                index_html_content += f'<div><h4>{category_name.replace("_", " ").title()} Layers -> Themes -> Feature Types </h4><img src="images/model/{model_filename}" alt="Model for {category_name}" style="width: auto; height: 500px;"></div>\n'
        index_html_content += "</div>\n"

    for category_name, summary_info in category_items:
        feature_types = summary_info["feature_types"]
        index_html_content += f"""
                    <li class="table-item">
                        <a href="topographic_layers_metadata.html#{category_name}">{category_name}</a>
                        <div class="feature-count">{len(feature_types)} feature types: {", ".join(feature_types)}</div>
                    </li>
            """

    index_html_content += """
                </ul>
            </div>

        </body>
        </html>
        """

    # Write index HTML file
    index_file_path = os.path.join(html_output_folder, "index.html")
    with open(index_file_path, "w", encoding="utf-8") as f:
        f.write(index_html_content)

    print(f"Index page created: {index_file_path}")

    print(f"HTML documentation created: {html_file_path}")
    return html_file_path


if __name__ == "__main__":
    model_folder = r"C:\Data\Model"
    metadata_folder = os.path.join(model_folder, "metadata")
    source_metadata_folder = os.path.join(model_folder, "metadata_source")
    metadata_info_file = os.path.join(source_metadata_folder, "metadata_info.json")
    html_output_folder = os.path.join(metadata_folder, "html")

    if not os.path.exists(metadata_folder):
        os.makedirs(metadata_folder)
    if not os.path.exists(html_output_folder):
        os.makedirs(html_output_folder)

    if not os.path.exists(metadata_info_file):
        raise FileNotFoundError(f"metadata_info.json not found at: {metadata_info_file}")

    with open(metadata_info_file, "r", encoding="utf-8") as f:
        table_dictionary = json.load(f)

    # Create the HTML documentation
    create_html_file(table_dictionary, html_output_folder)
    create_markdown_file(table_dictionary, html_output_folder)
