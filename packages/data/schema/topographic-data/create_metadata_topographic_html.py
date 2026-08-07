import os
import shutil
import json

def create_html_file(table_dictionary, html_output_folder):
    """
    Create a static HTML file with all feature information and copy images for self-contained package.
    """

    # Create images subdirectories
    html_images_folder = os.path.join(html_output_folder, "images")
    model_images_folder = os.path.join(html_images_folder, "model")
    feature_images_folder = os.path.join(html_images_folder, "features")
    aerial_images_folder = os.path.join(html_images_folder, "aerial")

    os.makedirs(model_images_folder, exist_ok=True)
    os.makedirs(feature_images_folder, exist_ok=True)
    os.makedirs(aerial_images_folder, exist_ok=True)

    # Start HTML content
    html_content = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Topo50 Features Documentation</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                .table-section { margin-bottom: 40px; border: 1px solid #ddd; padding: 20px; }
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
            html_content += f'<div><h4>Model Schema</h4><img src="images/model/{model_filename}" alt="Model for {category_name}" style="max-width: 800px; height: 500px;"></div>\n'
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
            feature_src = feature_info["feature_picture_path"]
            aerial_src = feature_info["aerial_picture_path"]
            if os.path.exists(feature_src):
                feature_filename = os.path.basename(feature_src)
                feature_dest = os.path.join(feature_images_folder, feature_filename)
                shutil.copy2(feature_src, feature_dest)
                html_content += f'<div><h4>Feature Visualization</h4><img src="images/features/{feature_filename}" alt="Feature {feature_type}" style="border: none;"></div>\n'
            # Copy and reference aerial picture
            if os.path.exists(aerial_src):
                aerial_filename = os.path.basename(aerial_src)
                aerial_dest = os.path.join(aerial_images_folder, aerial_filename)
                shutil.copy2(aerial_src, aerial_dest)
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
    summary_data = table_dictionary["summary"]
    table_dictionary.pop("summary")

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
        + str(len(table_dictionary))
        + """</p>
                <p><strong>Total Feature Types:</strong> """
        + str(
            sum(
                len(table_info["feature_types"])
                for table_info in table_dictionary.values()
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
        model_src = summary_info[2]
        if os.path.exists(model_src):
            model_filename = os.path.basename(model_src)
            model_dest = os.path.join(model_images_folder, model_filename)
            shutil.copy2(model_src, model_dest)

            if category_name == "Buildings_and_Structures":
                index_html_content += f'<div><h4>{category_name.replace("_", " ").title()} Layers -> Themes -> Feature Types </h4><img src="images/model/{model_filename}" alt="Model for {category_name}" style="width: auto; height: 500px;"></div>\n'
            elif category_name == "Administrative_and_Reference":
                index_html_content += f'<div><h4>{category_name.replace("_", " ").title()} Layers -> Themes -> Feature Types </h4><img src="images/model/{model_filename}" alt="Model for {category_name}" style="width: auto; height: 200px;"></div>\n'
            elif category_name == "Land_Cover_and_Land_Use":
                index_html_content += f'<div><h4>{category_name.replace("_", " ").title()} Layers -> Themes -> Feature Types </h4><img src="images/model/{model_filename}" alt="Model for {category_name}" style="width: auto; height: 700px;"></div>\n'
            elif category_name == "Terrain":
                index_html_content += f'<div><h4>{category_name.replace("_", " ").title()} Layers -> Themes -> Feature Types </h4><img src="images/model/{model_filename}" alt="Model for {category_name}" style="width: auto; height: 250px;"></div>\n'
            elif category_name == "Transport_and_Infrastructure":
                index_html_content += f'<div><h4>{category_name.replace("_", " ").title()} Layers -> Themes -> Feature Types </h4><img src="images/model/{model_filename}" alt="Model for {category_name}" style="width: auto; height: 700px;"></div>\n'
            elif category_name == "Water_and_Marine":
                index_html_content += f'<div><h4>{category_name.replace("_", " ").title()} Layers -> Themes -> Feature Types </h4><img src="images/model/{model_filename}" alt="Model for {category_name}" style="width: auto; height: 400px;"></div>\n'
            else:
                index_html_content += f'<div><h4>{category_name.replace("_", " ").title()} Layers -> Themes -> Feature Types </h4><img src="images/model/{model_filename}" alt="Model for {category_name}" style="width: auto; height: 500px;"></div>\n'
        index_html_content += "</div>\n"

    for category_name, summary_info in table_dictionary.items():
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
