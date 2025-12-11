import os
import pandas as pd
import shutil
import json
import sqlite3


def get_object_description(conn, name):
    cursor = conn.cursor()

    # Execute the SQL query
    cursor.execute(
        """
    SELECT
        o.id AS objectclass_id,
        o.name,
        o.entityclass,
        o.objectinheritance AS geometry_type,
        d.text AS description_text
    FROM objectclass o
    LEFT JOIN description d ON d.objectclass_id = o.id
    WHERE d.type = 'description'
    AND o.name = ?;
    """,
        (name,),
    )

    return cursor.fetchall()


def create_features_dict(
    conn,
    layer_descriptions_file,
    features_bythemes_file,
    schema_fields_folder,
    layer_pictures_folder,
    aerial_pictures_folder,
    model_themes_folder,
):
    """
    Create a dictionary of features organized by table from an Excel file (version 2 - direct creation).

    Args:
        conn (sqlite3.Connection): SQLite database connection
        layer_descriptions_file (str): Path to the CSV file containing layer descriptions
        features_bythemes_file (str): Path to the Excel file containing table, theme, and feature_type data
        schema_fields_folder (str): Path to the folder containing schema field pictures
        layer_pictures_folder (str): Path to the folder containing layer pictures
        aerial_pictures_folder (str): Path to the folder containing aerial pictures
        model_themes_folder (str): Path to the folder containing model themes

    Returns:
        dict: Dictionary organized by table containing feature information
    """
    # Read the features by themes Excel file
    themes_df = pd.read_excel(features_bythemes_file)

    # Read layer descriptions CSV
    layer_desc_df = None
    if os.path.exists(layer_descriptions_file):
        layer_desc_df = pd.read_csv(layer_descriptions_file)

    # Create dictionary organized by table
    table_dictionary = {}
    summary_dictionary = {}

    for _, row in themes_df.iterrows():
        table = row["table"]
        theme = row["theme"]
        feature_type = row["feature_type"]

        # Determine table_lds and table_type
        if table.endswith("_point"):
            table_lds = feature_type + "_pnt"
            table_type = "Point"
        elif table.endswith("_line"):
            if feature_type.endswith("_edge"):
                table_lds = feature_type
                table_type = "Line"
            elif feature_type in ["coastline", "contour"]:
                table_lds = feature_type
                table_type = "Line"
            else:
                table_lds = feature_type + "_cl"
                table_type = "Line"
        else:
            if table in ["descriptive_text", "geographic_name"]:
                table_lds = feature_type
                table_type = "Point Annotation"
            elif table in ["tree_locations"]:
                table_lds = feature_type + "_pnt"
                table_type = "Point"
            else:
                table_lds = feature_type + "_poly"
                table_type = "Polygon"

        # Get feature description from database
        descriptions = get_object_description(conn, table_lds)
        feature_description_text = descriptions[0][4] if descriptions else ""
        feature_description_text = feature_description_text.replace("_x000D_", "")
        feature_description_text = feature_description_text.replace("  ", " ")
        feature_description_text = feature_description_text.replace("\r", "")

        # Get layer description from CSV file
        layer_description_text = ""
        if layer_desc_df is not None:
            matching_desc = layer_desc_df[layer_desc_df["layer"] == table]
            if not matching_desc.empty:
                layer_description_text = matching_desc.iloc[0]["description"]
                layer_category = matching_desc.iloc[0]["category"]
                model_picture_path = os.path.join(schema_fields_folder, f"{table}.png")
                category = layer_category.replace(" & ", "_and_").replace(" ", "_")
                model_summary_picture_path = os.path.join(
                    model_themes_folder, f"{category}_summary_diagram.svg"
                )
                model_theme_picture_path = os.path.join(
                    model_themes_folder, f"{category}_diagram.svg"
                )

        if layer_category not in summary_dictionary:
            summary_dictionary[category] = [
                layer_category,
                model_summary_picture_path,
                model_theme_picture_path,
            ]

        # Initialize table entry if not exists
        if table not in table_dictionary:
            table_dictionary[table] = {
                "feature_types": [],
                "layer_description": layer_description_text,
                "layer_category": layer_category,
                "model_picture_path": model_picture_path,
                "model_summary_picture_path": model_summary_picture_path,
                "model_theme_picture_path": model_theme_picture_path,
                "features": {},
            }

        # Add feature type to list if not already present
        if feature_type not in table_dictionary[table]["feature_types"]:
            table_dictionary[table]["feature_types"].append(feature_type)

        # Add feature details
        table_dictionary[table]["features"][feature_type] = {
            "theme": theme,
            "table_lds": table_lds,
            "table_type": table_type,
            "description": feature_description_text,
            "feature_picture_path": os.path.join(
                layer_pictures_folder, f"{table_lds}.gif"
            ),
            "aerial_picture_path": os.path.join(
                aerial_pictures_folder, f"{table_lds}.gif"
            ),
        }

        # Sort table_dictionary by table name
        table_dictionary = dict(sorted(table_dictionary.items()))
        summary_dictionary = dict(sorted(summary_dictionary.items()))

    return table_dictionary, summary_dictionary


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
            html_content += f'<div><h4>Model Schema</h4><img src="images/model/{model_filename}" alt="Model for {category_name}" style="max-width: 200px; height: auto;"></div>\n'
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
            <h1>Topo50 Features Documentation</h1>
            
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
                index_html_content += f'<div><h4>{category_name.replace("_", " ").title()} Layers -> Themes -> Feature Types </h4><img src="images/model/{model_filename}" alt="Model for {category_name}" style="width: auto; height: 350px;"></div>\n'
            else:
                index_html_content += f'<div><h4>{category_name.replace("_", " ").title()} Layers -> Themes -> Feature Types </h4><img src="images/model/{model_filename}" alt="Model for {category_name}" style="width: auto; height: auto;"></div>\n'
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
    schema_fields_folder = os.path.join(source_metadata_folder, "model-diagrams", "png")
    layer_pictures_folder = os.path.join(source_metadata_folder, "diagrams")
    aerial_pictures_folder = os.path.join(source_metadata_folder, "airphotos")
    catalog_db = os.path.join(source_metadata_folder, "catalog.db")
    layer_descriptions_file = os.path.join(
        source_metadata_folder, "layer_descriptions.csv"
    )
    model_themes_folder = os.path.join(source_metadata_folder, "model-themes")
    # outputs
    metadata_info_file = os.path.join(metadata_folder, "metadata_info.json")
    html_output_folder = os.path.join(metadata_folder, "html")

    conn = sqlite3.connect(catalog_db)

    if not os.path.exists(metadata_folder):
        os.makedirs(metadata_folder)
    if not os.path.exists(html_output_folder):
        os.makedirs(html_output_folder)

    features_bythemes_file = os.path.join(model_folder, "schema_features_theme.xlsx")

    table_dictionary, summary_dictionary = create_features_dict(
        conn,
        layer_descriptions_file,
        features_bythemes_file,
        schema_fields_folder,
        layer_pictures_folder,
        aerial_pictures_folder,
        model_themes_folder,
    )
    # Append summary_dictionary to table_dictionary
    table_dictionary["summary"] = summary_dictionary
    # Write features_dict to JSON file
    with open(metadata_info_file, "w", encoding="utf-8") as f:
        json.dump(table_dictionary, f, indent=4, ensure_ascii=False)

    print(f"Features dictionary saved to: {metadata_info_file}")

    # Create the HTML documentation
    create_html_file(table_dictionary, html_output_folder)
