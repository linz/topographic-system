import os
import pandas as pd
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
    Create a dictionary of features organized by table from a CSV file (version 2 - direct creation).

    Args:
        conn (sqlite3.Connection): SQLite database connection
        layer_descriptions_file (str): Path to the CSV file containing layer descriptions
        features_bythemes_file (str): Path to the CSV file containing table, theme, and feature_type data
        schema_fields_folder (str): Path to the folder containing schema field pictures
        layer_pictures_folder (str): Path to the folder containing layer pictures
        aerial_pictures_folder (str): Path to the folder containing aerial pictures
        model_themes_folder (str): Path to the folder containing model themes

    Returns:
        dict: Dictionary organized by table containing feature information
    """
    # Read the features by themes CSV file
    themes_df = pd.read_csv(features_bythemes_file)

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

if __name__ == "__main__":
    model_folder = r"C:\Data\Model"
    source_metadata_folder = os.path.join(model_folder, "metadata_source")
    schema_fields_folder = os.path.join(source_metadata_folder, "model-diagrams", "png")
    layer_pictures_folder = os.path.join(source_metadata_folder, "diagrams")
    aerial_pictures_folder = os.path.join(source_metadata_folder, "airphotos")
    model_themes_folder = os.path.join(source_metadata_folder, "model-themes")
    catalog_db = os.path.join(source_metadata_folder, "catalog.db")
    layer_descriptions_file = os.path.join(
        source_metadata_folder, "layer_descriptions.csv"
    )
    features_bythemes_file = os.path.join(source_metadata_folder, "schema_features_theme.csv")

    # outputs
    metadata_info_file = os.path.join(source_metadata_folder, "metadata_info.json")

    conn = sqlite3.connect(catalog_db)

 
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

