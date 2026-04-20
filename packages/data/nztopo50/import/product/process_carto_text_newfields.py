import pandas as pd
import geopandas as gpd
import numpy as np
import os
import logging
from datetime import datetime
from shapely.geometry import MultiLineString


class CartoTextProcessor:
    """
    Class to manage carto text processing operations with comprehensive logging.
    """

    def __init__(
        self, output_directory, product_database, carto_text_layer, logs_csv_folder
    ):
        """
        Initialize the CartoTextProcessor with logging setup.

        Parameters:
        - output_directory: Directory for output files and logs
        - product_database: Path to the product database
        - carto_text_layer: Path to the carto text layer
        - logs_csv_folder: Folder to save CSV logs
        """
        self.output_directory = output_directory
        self.product_database = product_database
        self.carto_text_layer = carto_text_layer
        self.logs_csv_folder = logs_csv_folder

        self.logger = self._setup_logging()
        self.unmatched_ids = []
        self.multiple_value_rows = []

        # Define field specifications for new fields
        self.field_specs = [
            ("font", "string", 20),
            ("style", "string", 30),
            ("colour", "string", 20),
            ("size", "float64", None),
            ("placement", "string", 2),
            ("offset", "float64", None),
            ("textanchor", "string", 20),
            ("labelanchor", "float64", None),
            ("charplace", "string", 32),
            ("chardistance", "float64", None),
        ]

        self.logger.info(
            f"CartoTextProcessor initialized with logs CSV folder: {logs_csv_folder}"
        )

    def _setup_logging(self):
        """
        Set up logging to both file and console.
        """
        # Create logs CSV folder if it doesn't exist
        os.makedirs(self.logs_csv_folder, exist_ok=True)

        # Create logger
        logger = logging.getLogger("CartoTextProcessor")
        logger.setLevel(logging.INFO)

        # Clear any existing handlers
        logger.handlers.clear()

        # Create formatters
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # File handler
        log_file = os.path.join(
            self.logs_csv_folder,
            f"carto_text_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
        )
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)

        # Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        logger.info(f"Logging initialized. Log file: {log_file}")
        return logger

    def read_and_process_tabs(self, mapping_spreadsheet, full_layers_sheet):
        """
        Read the Excel file and process 'Full layers' sheet.
        Skip fully blank lines and process sections starting with 'ATT' in layer column.
        """
        try:
            self.logger.info(
                f"Reading Excel file: {mapping_spreadsheet}, sheet: {full_layers_sheet}"
            )

            # Read the Excel file and specific sheet
            df = pd.read_excel(mapping_spreadsheet, sheet_name=full_layers_sheet)

            # Remove completely empty rows (all NaN values)
            df = df.dropna(how="all")

            # Find the column that contains layer information (assuming it's the first column or named 'layer')
            layer_column = df.columns[0]  # Assuming layer info is in first column

            # Process ATT sections
            att_sections = []
            current_section = []
            current_att_name = None

            for index, row in df.iterrows():
                row_value = (
                    str(row[layer_column]).strip()
                    if pd.notna(row[layer_column])
                    else ""
                )

                # Check if this row starts a new ATT section
                if row_value.startswith("ATT"):
                    # Save previous section if it exists
                    if current_section:
                        att_sections.append(
                            {"att_name": current_att_name, "data": current_section}
                        )

                    # Start new section
                    current_att_name = row_value
                    current_section = [row]
                    self.logger.info(f"Found ATT section: {current_att_name}")

                elif current_att_name:  # We're inside an ATT section
                    # Add row to current section until we hit next ATT or end
                    current_section.append(row)

            # Don't forget the last section
            if current_section:
                att_sections.append(
                    {"att_name": current_att_name, "data": current_section}
                )

            self.logger.info(f"Processed {len(att_sections)} ATT sections")

            # Initialize DataFrame to collect all processed rows
            formatted_rows = pd.DataFrame()

            # Process each ATT section
            for section in att_sections:
                section_row = self.process_att_section(
                    section["att_name"], section["data"]
                )

                # Append section_row to formatted_rows
                if section_row is not None and not section_row.empty:
                    formatted_rows = pd.concat(
                        [formatted_rows, section_row], ignore_index=True
                    )
                    self.logger.info(
                        f"Added {len(section_row)} rows from {section['att_name']} to formatted_rows"
                    )

            self.logger.info(f"Total formatted rows: {len(formatted_rows)}")
            return formatted_rows

        except FileNotFoundError:
            self.logger.error(f"Could not find file {mapping_spreadsheet}")
            return None
        except Exception as e:
            self.logger.error(f"Error processing Excel file: {e}")
            return None

    def process_att_section(self, att_name, section_data):
        """
        Process individual ATT section data.
        """
        self.logger.info(f"Processing section: {att_name}")
        self.logger.info(f"Number of rows in section: {len(section_data)}")

        # Convert section data to DataFrame for easier processing
        section_df = pd.DataFrame(section_data)

        # Display the columns available for processing
        self.logger.info(f"Available columns: {list(section_df.columns)}")

        # Process the section data here - modify section_data in place
        # Each ATT section should be condensed from 4 rows (ATT header + 3 data rows) to 1 row

        # Check if we have at least 4 rows (ATT header + 3 data rows)
        if len(section_data) >= 4:
            # Get the ATT header row (index 0) and next 3 data rows
            att_header_row = section_data[0]  # ATT header row
            data_row1 = section_data[1]  # First data row
            data_row2 = section_data[2]  # Second data row
            data_row3 = section_data[3]  # Third data row

            self.logger.debug(f"ATT Header row: {att_header_row}")
            self.logger.info("Processing 4 rows: ATT header + 3 data rows")

            # Extract SYMBOL field from all 4 rows
            symbol_values = []
            for row in [att_header_row, data_row1, data_row2, data_row3]:
                if "SYMBOL" in section_df.columns:
                    symbol_col_index = section_df.columns.get_loc("SYMBOL")
                    symbol_value = (
                        str(row.iloc[symbol_col_index])
                        if pd.notna(row.iloc[symbol_col_index])
                        else ""
                    )
                    symbol_value = symbol_value.strip()
                    if symbol_value and symbol_value != "nan":
                        symbol_values.append(symbol_value)

            # Combine all SYMBOL values into the ATT header row
            if symbol_values:
                combined_symbol = " ".join(symbol_values)

                # Update the ATT header row with combined SYMBOL and other fields
                att_header_row.loc["SYMBOL"] = combined_symbol

                self.logger.info(f"Combined SYMBOL from 4 rows: '{combined_symbol}'")

                # Replace the 4 rows with just the single combined ATT header row
                section_data = [att_header_row]

                self.logger.info("Section condensed from 4 rows to 1 row")
            else:
                self.logger.warning(
                    "No valid SYMBOL values found, keeping original ATT header row"
                )
                section_data = [att_header_row]
        else:
            self.logger.warning(
                f"Section has only {len(section_data)} rows, expected at least 4 for processing"
            )

        # Update section_df with the final modified section_data
        section_df = pd.DataFrame(section_data)

        # Clean up all columns except LAYER and SECTION - remove single quotes
        for col in section_df.columns:
            if col not in ["LAYER", "SECTION"]:
                section_df[col] = (
                    section_df[col].astype(str).str.replace("'", "", regex=False)
                )
                # self.logger.info(f"Removed single quotes from {col} column")

        self.logger.info(f"Final section has {len(section_data)} rows after processing")

        return section_df

    def process_new_values_tab(self, mapping_spreadsheet, new_values_sheet):
        """
        Read and process 'New values' sheet.
        For column 'Text Bend', if value is empty, set it to 0.
        """
        try:
            self.logger.info(f"Processing New values sheet: {new_values_sheet}")

            # Read the Excel file and specific sheet
            df = pd.read_excel(mapping_spreadsheet, sheet_name=new_values_sheet)

            self.logger.info(f"Read {len(df)} rows from '{new_values_sheet}' sheet")
            self.logger.info(f"Available columns: {list(df.columns)}")

            # Process 'Text Bend' column - set empty values to 0
            if "Text Bend" in df.columns:
                # Count empty values before processing
                empty_count = (
                    df["Text Bend"].isna().sum() + (df["Text Bend"] == "").sum()
                )
                self.logger.info(
                    f"Found {empty_count} empty values in 'Text Bend' column"
                )

                # Fill empty values with 0
                df["Text Bend"] = df["Text Bend"].fillna(0)
                df.loc[df["Text Bend"] == "", "Text Bend"] = 0

                self.logger.info(f"Set {empty_count} empty 'Text Bend' values to 0")
            else:
                self.logger.warning("'Text Bend' column not found in the sheet")

            # Export processed data
            output_file = os.path.join(self.logs_csv_folder, "new_values_processed.csv")
            df.to_csv(output_file, index=False)
            self.logger.info(f"Exported processed 'New values' data to {output_file}")

            return output_file

        except FileNotFoundError:
            self.logger.error(f"Could not find file {mapping_spreadsheet}")
            return None
        except Exception as e:
            self.logger.error(f"Error processing 'New values' sheet: {e}")
            return None

    def process_font_mapping_tab(self, mapping_spreadsheet, font_mapping_sheet):
        """
        Read and process 'Font mapping' sheet.
        - If LAMPS column is empty, write 'empty'
        - If FONT column is empty, write 'empty'
        - If LAMPS = 'Others:', skip the row
        """
        try:
            self.logger.info(f"Processing Font mapping sheet: {font_mapping_sheet}")

            # Read the Excel file and specific sheet
            df = pd.read_excel(mapping_spreadsheet, sheet_name=font_mapping_sheet)

            self.logger.info(f"Read {len(df)} rows from '{font_mapping_sheet}' sheet")
            self.logger.info(f"Available columns: {list(df.columns)}")

            # Check if required columns exist
            if "LAMPS" not in df.columns:
                self.logger.warning("'LAMPS' column not found in the sheet")
                return None
            if "FONT" not in df.columns:
                self.logger.warning("'FONT' column not found in the sheet")
                return None

            # Skip rows where LAMPS = 'Others:'
            initial_count = len(df)
            df = df[df["LAMPS"] != "Others:"]
            skipped_count = initial_count - len(df)
            self.logger.info(f"Skipped {skipped_count} rows where LAMPS = 'Others:'")

            # Process LAMPS column - set empty values to 'empty'
            empty_lamps_count = df["LAMPS"].isna().sum() + (df["LAMPS"] == "").sum()
            df["LAMPS"] = df["LAMPS"].fillna("empty")
            df.loc[df["LAMPS"] == "", "LAMPS"] = "empty"
            self.logger.info(f"Set {empty_lamps_count} empty 'LAMPS' values to 'empty'")

            # Process FONT column - set empty values to 'empty'
            empty_font_count = df["FONT"].isna().sum() + (df["FONT"] == "").sum()
            df["FONT"] = df["FONT"].fillna("empty")
            df.loc[df["FONT"] == "", "FONT"] = "empty"
            self.logger.info(f"Set {empty_font_count} empty 'FONT' values to 'empty'")

            # Export processed data
            output_file = os.path.join(
                self.logs_csv_folder, "font_mapping_processed.csv"
            )
            df.to_csv(output_file, index=False)
            self.logger.info(f"Exported processed 'Font mapping' data to {output_file}")

            return output_file

        except FileNotFoundError:
            self.logger.error(f"Could not find file {mapping_spreadsheet}")
            return None
        except Exception as e:
            self.logger.error(f"Error processing 'Font mapping' sheet: {e}")
            return None

    def process_fulllayers_tab(self, mapping_spreadsheet, full_layers_sheet):
        """
        Process the full layers tab and export results.
        """
        formatted_rows = self.read_and_process_tabs(
            mapping_spreadsheet, full_layers_sheet
        )

        if formatted_rows is not None:
            self.logger.info(
                f"Successfully processed {len(formatted_rows)} formatted rows from {full_layers_sheet} sheet"
            )

            # Export processed sections to CSV
            output_file = os.path.join(self.logs_csv_folder, "formatted_rows.csv")
            formatted_rows.to_csv(output_file, index=False)
            self.logger.info(f"Exported formatted_rows to {output_file}")

            return output_file

        else:
            self.logger.error("Failed to process the Excel file")
            return None

    def validate_and_assign_field(self, gdf, field_name, values):
        """
        Validate and assign values to a field with type and length constraints.

        Parameters:
        - gdf: GeoDataFrame with _field_specs attribute
        - field_name: Name of the field to update
        - values: Values to assign (Series, list, or scalar)
        """
        if not hasattr(gdf, "_field_specs") or field_name not in gdf._field_specs:
            self.logger.warning(f"No field specification found for {field_name}")
            gdf[field_name] = values
            return

        field_type, max_length = gdf._field_specs[field_name]

        if field_type == "string":
            # Convert to pandas Series if needed
            if not isinstance(values, pd.Series):
                values = pd.Series(values, index=gdf.index)

            # Truncate strings and enforce type
            values = values.astype(str).str.slice(0, max_length)
            # Replace any NaN/NA values with empty strings
            values = values.fillna("")

            # Check for truncation
            original_max = values.str.len().max() if not values.isna().all() else 0
            if max_length and original_max > max_length:
                self.logger.warning(
                    f"Truncated {field_name} values to {max_length} characters"
                )

        elif field_type == "float64":
            # Ensure float64 type
            values = pd.Series(values, index=gdf.index, dtype=np.float64)

        gdf[field_name] = values
        self.logger.info(
            f"Assigned values to {field_name} with type {field_type}"
            + (f", max length {max_length}" if max_length else "")
        )

    def _enforce_field_constraints(self, gdf):
        """
        Validate and enforce field constraints before export.
        Truncates strings that exceed maximum length and ensures proper data types.

        Parameters:
        - gdf: GeoDataFrame with _field_specs attribute containing field constraints
        """

        def enforce_string_length(series, max_length):
            """Truncate strings to maximum length and ensure proper string type"""
            if max_length is not None:
                result = series.astype(str).str.slice(0, max_length)
            else:
                result = series.astype(str)
            # Replace any NaN/NA values with empty strings
            return result.fillna("")

        self.logger.info("Enforcing field constraints before export...")
        for field_name, (field_type, max_length) in gdf._field_specs.items():
            if field_type == "string" and max_length is not None:
                # Truncate strings that exceed maximum length
                original_length = (
                    gdf[field_name].str.len().max()
                    if not gdf[field_name].isna().all()
                    else 0
                )
                gdf[field_name] = enforce_string_length(gdf[field_name], max_length)
                if original_length > max_length:
                    self.logger.warning(
                        f"Truncated {field_name} values from max {original_length} to {max_length} characters"
                    )
            elif field_type == "float64":
                # Ensure float64 type
                gdf[field_name] = gdf[field_name].astype(np.float64)

            # Final safety check: replace any remaining <NA> strings with empty strings
            if field_type == "string":
                if hasattr(gdf[field_name], "str"):
                    gdf[field_name] = gdf[field_name].astype(str).replace("<NA>", "")
                gdf[field_name] = gdf[field_name].fillna("")

    def parse_symbol_text(self, symbol_text):
        """
        Parse SYMBOL text and extract field=value conditions.

        Example input: "text_font = ATTriumMou-Cond and text_colour = 9 and text_height = 0.0013 and text_place in (34)"
        Returns: dict of field: value pairs
        """
        conditions = {}

        if not symbol_text or pd.isna(symbol_text):
            return conditions

        # Split by 'and' to get individual conditions
        parts = str(symbol_text).split(" and ")

        for part in parts:
            part = part.strip()

            # Handle 'field in (value)' format
            if " in (" in part and part.endswith(")"):
                field, value_part = part.split(" in (", 1)
                field = field.strip()
                value = value_part.rstrip(")").strip()

                # Convert to appropriate type
                if value.replace(".", "").replace("-", "").isdigit():
                    if "." in value:
                        conditions[field] = float(value)
                    else:
                        conditions[field] = int(value)
                else:
                    conditions[field] = value.strip("'\"")

            # Handle 'field = value' format
            elif " = " in part:
                field, value = part.split(" = ", 1)
                field = field.strip()
                value = value.strip()

                # Convert to appropriate type
                if value.replace(".", "").replace("-", "").isdigit():
                    if "." in value:
                        conditions[field] = float(value)
                    else:
                        conditions[field] = int(value)
                else:
                    conditions[field] = value.strip("'\"")

        return conditions

    def process_symbol_text_to_queries(self, full_layers_df, gdf):
        """
        Process SYMBOL text from full_layers CSV and convert to pandas queries.
        Apply these queries to filter the GDF layer.
        """
        queries_processed = []

        for index, row in full_layers_df.iterrows():
            if "SYMBOL" not in row or pd.isna(row["SYMBOL"]):
                continue

            symbol_text = row["SYMBOL"]
            self.logger.info(f"Processing SYMBOL: {symbol_text}")

            full_layer_id = row.get("LAYER", "Unknown Layer")
            self.logger.info(f"  Layer ID: {full_layer_id}")

            full_layer_font = row.get("FONT", "Unknown FONT")
            full_layer_style = row.get("STYLE", "Unknown STYLE")
            full_layer_colour = row.get("COLOUR", "Unknown COLOUR")
            full_layer_size = row.get("SIZE", "Unknown SIZE")
            full_layer_offset = row.get("OFFSET", "Unknown OFFSET")
            full_layer_placement = row.get("PLACEMENT", "Unknown PLACEMENT")

            field_values = {
                "layer_id": full_layer_id,
                "font": full_layer_font,
                "style": full_layer_style,
                "colour": full_layer_colour,
                "size": full_layer_size,
                "offset": full_layer_offset,
                "placement": full_layer_placement,
            }

            # Parse the symbol text to extract conditions
            conditions = self.parse_symbol_text(symbol_text)

            if not conditions:
                self.logger.info("  No valid conditions found")
                continue

            self.logger.info(f"  Parsed conditions: {conditions}")

            # Build pandas query string
            query_parts = []
            for field, value in conditions.items():
                if field == "text_place":
                    field = "text_placement"
                    # "1,4" -> text_placement = 1 or text_placement = 4
                    if isinstance(value, str) and "," in value:
                        values = value.split(",")
                        query_parts.append(
                            "(" + " or ".join([f"{field} == {v}" for v in values]) + ")"
                        )
                    else:
                        if isinstance(value, str):
                            query_parts.append(f"{field} == '{value}'")
                        else:
                            query_parts.append(f"{field} == {value}")
                else:
                    if isinstance(value, str):
                        query_parts.append(f"{field} == '{value}'")
                    else:
                        query_parts.append(f"{field} == {value}")

            if query_parts:
                query_string = " and ".join(query_parts)
                self.logger.info(f"  Generated query: {query_string}")

                try:
                    # Apply query to filter GDF
                    filtered_gdf = gdf.query(query_string)
                    self.logger.info(f"  Query matched {len(filtered_gdf)} features")

                    queries_processed.append(
                        {
                            "symbol_text": symbol_text,
                            "conditions": conditions,
                            "text_placement_query": query_parts[3]
                            if len(query_parts) > 3
                            else "N/A",
                            "query_string": query_string,
                            "matched_features": len(filtered_gdf),
                            "feature_indices": filtered_gdf.index.tolist(),
                            "field_values": field_values,
                        }
                    )

                except Exception as e:
                    self.logger.error(f"  Query failed: {e}")
                    # Try individual field checks for debugging
                    for field, value in conditions.items():
                        if field in gdf.columns:
                            field_type = gdf[field].dtype
                            unique_vals = gdf[field].unique()[
                                :10
                            ]  # Show first 10 unique values
                            self.logger.info(
                                f"    Field '{field}' exists, type: {field_type}, sample values: {unique_vals}"
                            )
                        else:
                            self.logger.warning(
                                f"    Field '{field}' does not exist in GDF"
                            )

        return queries_processed

    def update_layer_fields_from_queries(
        self,
        gdf,
        processed_queries,
        full_layers,
        new_values,
        font_mapping,
        new_font_name,
    ):
        """
        Update the new fields in GDF based on processed queries and CSV data.

        Complex workflow:
        1. For each query, get matched features
        2. Group by text_bend values
        3. For each text_bend, lookup new_values and font_mapping
        4. Update the new fields
        """
        if not processed_queries or new_values is None or font_mapping is None:
            self.logger.warning("Cannot update fields - missing required data")
            return gdf

        self.logger.info("Starting complex field updates from queries...")

        for query_idx, query_data in enumerate(processed_queries):
            self.logger.info(
                f"Processing query {query_idx + 1}: {query_data['symbol_text']}"
            )

            if query_data["matched_features"] == 0:
                self.logger.info("  No matched features, skipping")
                msg = f"Base Query - {query_data['field_values']['layer_id']} - No matched features for query conditions: {query_data['conditions']}"
                self.unmatched_ids.append(msg)
                continue

            # Get the matched features
            matched_indices = query_data["feature_indices"]
            matched_features = gdf.loc[matched_indices]

            self.logger.info(
                f"  Matched {len(matched_features)} features for this index query"
            )

            if "text_bend" not in gdf.columns:
                self.logger.warning("  text_bend column not found in GDF")
                continue

            # {'layer_id': 'ATT-cond-black-height-13-tp-34-todo', 'font': nan, 'style': nan, 'colour': nan, 'size': nan, 'offset': nan, 'placement': nan}
            field_values = query_data["field_values"]

            # Group by text_bend values
            text_bend_groups = matched_features.groupby("text_bend")

            self.logger.info(f"  Found {len(text_bend_groups)} text_bend groups")

            for text_bend_value, group in text_bend_groups:
                self.logger.info(
                    f"    Processing text_bend = {text_bend_value} ({len(group)} features)"
                )

                # Query new_values_processed using text_bend value
                matching_new_values = new_values[
                    new_values["Text Bend"] == text_bend_value
                ]
                if matching_new_values.empty:
                    self.logger.warning(
                        f"      No matching new_values for text_bend = {text_bend_value}"
                    )
                    value_setting_info = "text_bend_value = {0}".format(text_bend_value)
                    self.unmatched_ids.append(
                        "Text Bend: "
                        + field_values["layer_id"]
                        + " - "
                        + value_setting_info
                    )
                    continue

                # we need style, colour text, size, text height
                # {'text_font': 'ATTriumMou-Cond', 'text_colour': 9, 'text_height': 0.0013, 'text_place': 34}
                # HEIGHT
                text_height = query_data["conditions"]["text_height"]
                matching_new_values = matching_new_values[
                    matching_new_values["Text Height"] == text_height
                ]
                if matching_new_values.empty:
                    self.logger.warning(
                        f"      No matching new_values for Text Height = {text_height}"
                    )
                    value_setting_info = (
                        "text_bend_value = {0}, text_height = {1}".format(
                            text_bend_value, text_height
                        )
                    )
                    self.unmatched_ids.append(
                        "Text Height: "
                        + field_values["layer_id"]
                        + " - "
                        + value_setting_info
                    )
                    continue

                text_colour_number = query_data["conditions"]["text_colour"]
                # COLOUR
                if text_colour_number == 9:
                    matching_new_values = matching_new_values[
                        matching_new_values["Colour"] == "black"
                    ]
                elif text_colour_number == 5:
                    matching_new_values = matching_new_values[
                        matching_new_values["Colour"] == "red"
                    ]
                elif text_colour_number == 6:
                    matching_new_values = matching_new_values[
                        matching_new_values["Colour"] == "steelblue"
                    ]
                else:
                    # 1 record has code in datafile - 1680
                    matching_new_values = matching_new_values[
                        matching_new_values["Colour"] == "black"
                    ]

                placement_value = query_data["text_placement_query"]

                if placement_value.startswith("(") and placement_value.endswith(")"):
                    # Handle multiple placement values (e.g., "(text_placement == 1 or text_placement == 4)")
                    placement_values = [
                        part.split("==")[1].strip()
                        for part in placement_value[1:-1].split(" or ")
                    ]
                    placement_values = [int(v) for v in placement_values]
                    matching_new_values = matching_new_values[
                        matching_new_values["Text Place"].isin(placement_values)
                    ]
                else:
                    placement_value = (
                        placement_value.split("==")[1].strip()
                        if "==" in placement_value
                        else placement_value
                    )
                    placement_value = int(placement_value)
                    matching_new_values = matching_new_values[
                        matching_new_values["Text Place"] == placement_value
                    ]

                if matching_new_values.empty:
                    self.logger.warning(
                        f"      No matching new_values for Placement = {placement_value}"
                    )
                    value_setting_info = "font_symbol = {0}, text_bend = {1}, text_height = {2}, text_colour = {3}, placement = {4}".format(
                        query_data["conditions"].get("text_font", None),
                        text_bend_value,
                        text_height,
                        text_colour_number,
                        placement_value,
                    )
                    self.unmatched_ids.append(
                        "Placement: "
                        + field_values["layer_id"]
                        + " - "
                        + value_setting_info
                    )
                    continue

                value_setting_info = "font_symbol = {0}, text_bend = {1}, text_height = {2}, colour = {3}, text placement = {4}".format(
                    query_data["conditions"].get("text_font", None),
                    text_bend_value,
                    text_height,
                    text_colour_number,
                    placement_value,
                )

                # Get FONT name from the current query's formatted_rows
                # We need to find the corresponding row in full_layers that matches this query
                font_style_name = None
                font_name = None
                if full_layers is not None:
                    # Try to match the query back to the full_layers row
                    matching_full_layers = full_layers[
                        full_layers["SYMBOL"] == query_data["symbol_text"]
                    ]
                    if not matching_full_layers.empty:
                        # font_name = matching_full_layers.iloc[0]['FONT'].strip()
                        font_style_name = query_data["conditions"].get(
                            "text_font", None
                        )
                        # style_name = matching_full_layers.iloc[0]['STYLE'].strip()
                        # if style_name == 'Condensed':
                        #    style_name = 'Cond'
                        # font_style_name = f"{font_name}-{style_name}"

                self.logger.info(f"      FONT from formatted_rows: {font_style_name}")

                # Lookup font in font_mapping_processed
                style_value = None
                if font_style_name:
                    matching_font = font_mapping[
                        font_mapping["LAMPS"] == font_style_name
                    ]
                    if not matching_font.empty:
                        # Get LAMPS font value and STYLE
                        lamps_font_value = matching_font.iloc[0].get("LAMPS", None)
                        style_value = matching_font.iloc[0].get("STYLE", None)
                        self.logger.info(
                            f"      Found LAMPS: {lamps_font_value}, STYLE: {style_value}"
                        )
                    else:
                        self.logger.warning(
                            f"      No matching font in font_mapping for: {font_name}"
                        )
                else:
                    self.logger.info("      FONT is nan/null, setting values to null")

                # STYLE query
                matching_new_values = matching_new_values[
                    matching_new_values["Style"] == style_value
                ]
                if matching_new_values.empty:
                    self.logger.warning(
                        f"      No matching new_values for Style = {style_value}"
                    )
                    value_setting_info = "font_symbol = {0}, text_bend = {1}, text_height = {2}, colour = {3}, text placement = {4}, style = {5}".format(
                        query_data["conditions"].get("text_font", None),
                        text_bend_value,
                        text_height,
                        text_colour_number,
                        placement_value,
                        style_value,
                    )
                    self.unmatched_ids.append(
                        "Style: "
                        + field_values["layer_id"]
                        + " - "
                        + value_setting_info
                    )
                    continue

                # For this implementation, we'll use the first matching row
                # You might need more complex logic to determine which row to use
                self.logger.info(
                    f"      Found {len(matching_new_values)} matching new_values rows"
                )
                if len(matching_new_values) > 1:
                    self.logger.warning(
                        f"      Multiple matching new_values rows found, using the first one. Details: {matching_new_values}"
                    )
                    self.multiple_value_rows.append(
                        {
                            "layer_id": field_values["layer_id"],
                            "font_symbol": query_data["conditions"].get(
                                "text_font", None
                            ),
                            "text_bend": text_bend_value,
                            "text_height": text_height,
                            "text_colour": text_colour_number,
                            "placement": placement_value,
                            "matching_rows": matching_new_values.to_dict(
                                orient="records"
                            ),
                        }
                    )
                new_values_row = matching_new_values.iloc[0]

                # Update the new fields for this group
                group_indices = group.index

                # Update fields using validate_and_assign_field
                if style_value:
                    # Update with actual values
                    gdf.loc[group_indices, "font"] = new_font_name
                    gdf.loc[group_indices, "style"] = style_value
                    gdf.loc[group_indices, "colour"] = new_values_row.get("Colour", "")
                    gdf.loc[group_indices, "size"] = pd.to_numeric(
                        new_values_row.get("Size", 0), errors="coerce"
                    )
                    gdf.loc[group_indices, "offset"] = pd.to_numeric(
                        new_values_row.get("Offset", 0), errors="coerce"
                    )
                    gdf.loc[group_indices, "placement"] = new_values_row.get(
                        "Placement", ""
                    )
                    gdf.loc[group_indices, "textanchor"] = new_values_row.get(
                        "Text Anchor", ""
                    )
                    gdf.loc[group_indices, "labelanchor"] = pd.to_numeric(
                        new_values_row.get("LabelAnchor", 0), errors="coerce"
                    )
                    gdf.loc[group_indices, "charplace"] = new_values_row.get(
                        "Char Place", ""
                    )
                    gdf.loc[group_indices, "chardistance"] = pd.to_numeric(
                        new_values_row.get("Char Dist", 0), errors="coerce"
                    )

                    self.logger.info(
                        f"      Updated {len(group_indices)} features with:"
                    )
                    self.logger.info(f"        FONT: {new_font_name}")
                    self.logger.info(f"        STYLE: {style_value}")
                    self.logger.info(
                        f"        COLOUR: {new_values_row.get('Colour', '')}"
                    )
                    self.logger.info(f"        SIZE: {new_values_row.get('Size', 0.0)}")
                    self.logger.info(
                        f"        OFFSET: {new_values_row.get('Offset', 0.0)}"
                    )
                    self.logger.info(
                        f"        PLACEMENT: {new_values_row.get('Placement', '')}"
                    )
                    self.logger.info(
                        f"        TEXT ANCHOR: {new_values_row.get('Text Anchor', '')}"
                    )
                    self.logger.info(
                        f"        LABEL ANCHOR: {new_values_row.get('LabelAnchor', 0.0)}"
                    )
                    self.logger.info(
                        f"        CHAR PLACE: {new_values_row.get('Char Place', '')}"
                    )
                    self.logger.info(
                        f"        CHAR DISTANCE: {new_values_row.get('Char Dist', 0.0)}"
                    )
                else:
                    self.unmatched_ids.append(
                        "Setting Zeros: "
                        + field_values["layer_id"]
                        + " - "
                        + value_setting_info
                    )
                    # Set to null/empty values (explicitly ensuring empty strings, not <NA>)
                    gdf.loc[group_indices, "font"] = ""
                    gdf.loc[group_indices, "style"] = ""
                    gdf.loc[group_indices, "colour"] = ""
                    gdf.loc[group_indices, "size"] = 0.0
                    gdf.loc[group_indices, "offset"] = 0.0
                    gdf.loc[group_indices, "placement"] = ""
                    gdf.loc[group_indices, "textanchor"] = ""
                    gdf.loc[group_indices, "labelanchor"] = 0.0
                    gdf.loc[group_indices, "charplace"] = ""
                    gdf.loc[group_indices, "chardistance"] = 0.0
                    self.logger.info(
                        f"      Set {len(group_indices)} features to null values"
                    )

        self.logger.info("Completed complex field updates")
        return gdf

    def process_carto_text_layer(
        self,
        carto_text_folder,
        product_database,
        carto_text_layer,
        full_layers_csv,
        new_values_csv,
        font_mapping_csv,
        new_font_name,
    ):
        """
        Process the carto_text layer using geopandas and the 3 processed CSV files.
        """
        try:
            # Construct the full path to the GeoPackage
            gpkg_path = os.path.join(carto_text_folder, product_database)

            if not os.path.exists(gpkg_path):
                self.logger.error(f"GeoPackage file {gpkg_path} does not exist")
                return None

            self.logger.info(f"Reading carto_text layer from: {gpkg_path}")

            # Read the carto_text layer using geopandas
            gdf = gpd.read_file(gpkg_path, layer=carto_text_layer)

            # Log initial geometry types
            geom_types = gdf.geom_type.value_counts()
            self.logger.info(f"Initial geometry types: {geom_types.to_dict()}")

            # Convert LineString to MultiLineString if present
            linestring_mask = gdf.geom_type == "LineString"
            if linestring_mask.any():
                linestring_count = linestring_mask.sum()
                self.logger.info(
                    f"Converting {linestring_count} LineString geometries to MultiLineString"
                )
                gdf.loc[linestring_mask, "geometry"] = gdf.loc[
                    linestring_mask, "geometry"
                ].apply(
                    lambda geom: MultiLineString([geom])
                    if geom.geom_type == "LineString"
                    else geom
                )

                # Log updated geometry types
                updated_geom_types = gdf.geom_type.value_counts()
                self.logger.info(
                    f"Updated geometry types: {updated_geom_types.to_dict()}"
                )
            else:
                self.logger.info("No LineString geometries found to convert")

            self.logger.info(f"Read {len(gdf)} features from {carto_text_layer} layer")
            self.logger.info(f"Available columns: {list(gdf.columns)}")

            # Add new fields to the GeoDataFrame with specific types and length constraints
            self.logger.info(
                "Adding new fields to carto_text layer with type constraints..."
            )

            # Add fields with proper data types
            for field_name, field_type, max_length in self.field_specs:
                if field_type == "string":
                    # Use object dtype for strings to avoid <NA> values
                    gdf[field_name] = pd.Series("", dtype="object", index=gdf.index)
                    self.logger.info(
                        f"Added {field_name}: string, max length {max_length}"
                    )
                elif field_type == "float64":
                    # Use float64 for real numbers
                    gdf[field_name] = pd.Series(0.0, dtype=np.float64, index=gdf.index)
                    self.logger.info(f"Added {field_name}: float64")

            # Store field specifications for later validation
            gdf._field_specs = {
                name: (dtype, max_len) for name, dtype, max_len in self.field_specs
            }

            self.logger.info("Field constraints applied successfully")

            # Load the processed CSV files
            full_layers = None
            new_values = None
            font_mapping = None
            if full_layers_csv and os.path.exists(full_layers_csv):
                full_layers = pd.read_csv(full_layers_csv)
                self.logger.info(f"Loaded full layers CSV: {len(full_layers)} rows")
            else:
                self.logger.warning("Full layers CSV not available")

            if new_values_csv and os.path.exists(new_values_csv):
                new_values = pd.read_csv(new_values_csv)
                self.logger.info(f"Loaded new values CSV: {len(new_values)} rows")
            else:
                self.logger.warning("New values CSV not available")

            if font_mapping_csv and os.path.exists(font_mapping_csv):
                font_mapping = pd.read_csv(font_mapping_csv)
                self.logger.info(f"Loaded font mapping CSV: {len(font_mapping)} rows")
            else:
                self.logger.warning("Font mapping CSV not available")

            # Process full_layers CSV - convert SYMBOL text to queries
            if full_layers is not None and "SYMBOL" in full_layers.columns:
                self.logger.info("Processing SYMBOL text from full_layers CSV...")
                processed_queries = self.process_symbol_text_to_queries(
                    full_layers, gdf
                )
                self.logger.info(
                    f"Generated {len(processed_queries)} queries from SYMBOL text"
                )

                # Store queries for potential further use
                gdf._symbol_queries = processed_queries

                # Update layer fields based on processed queries and CSV data
                gdf = self.update_layer_fields_from_queries(
                    gdf,
                    processed_queries,
                    full_layers,
                    new_values,
                    font_mapping,
                    new_font_name,
                )
            else:
                self.logger.warning(
                    "Cannot process SYMBOL text - full_layers CSV not available or SYMBOL column missing"
                )
                processed_queries = []

            # Example: Display basic statistics
            self.logger.info(
                f"Carto text layer geometry types: {gdf.geometry.type.value_counts().to_dict()}"
            )
            self.logger.info(f"Total columns after adding fields: {len(gdf.columns)}")

            # Display SYMBOL query processing results
            if hasattr(gdf, "_symbol_queries") and gdf._symbol_queries:
                self.logger.info("SYMBOL Query Processing Summary:")
                self.logger.info(f"Total queries processed: {len(gdf._symbol_queries)}")

                total_matches = sum(q["matched_features"] for q in gdf._symbol_queries)
                successful_queries = len(
                    [q for q in gdf._symbol_queries if q["matched_features"] > 0]
                )

                self.logger.info(f"Successful queries: {successful_queries}")
                self.logger.info(f"Total feature matches: {total_matches}")

                # Show some example queries
                self.logger.info("Sample processed queries:")
                for i, query in enumerate(gdf._symbol_queries[:3]):  # Show first 3
                    self.logger.info(f"  {i + 1}. '{query['symbol_text']}'")
                    self.logger.info(
                        f"     -> {query['query_string']} (matched {query['matched_features']} features)"
                    )

            # Validate and enforce field constraints before export
            ##self._enforce_field_constraints(gdf)

            # Create schema definition for file export (useful for GeoPackage/Shapefile)
            schema_properties = {}
            for field_name, (field_type, max_length) in gdf._field_specs.items():
                if field_type == "string":
                    schema_properties[field_name] = (
                        f"str:{max_length}" if max_length else "str"
                    )
                elif field_type == "float64":
                    schema_properties[field_name] = "float"

            self.logger.info(f"Schema properties: {schema_properties}")

            return gdf

        except Exception as e:
            self.logger.error(f"Error processing carto_text layer: {e}")
            return None

    def export_data(self, gdf, export_format="GPKG"):
        """
        Export the processed geodataframe to specified format.
        """

        if export_format == "GPKG":
            output_file = os.path.join(self.output_directory, self.product_database)
            try:
                gdf.to_file(
                    output_file,
                    driver="GPKG",
                    layer=self.carto_text_layer,
                    layer_options={"geometry_name": "geometry"},
                )
                self.logger.info(
                    f"Exported processed carto_text layer to {output_file}"
                )
            except Exception as e:
                self.logger.warning(f"Could not export to GeoPackage: {e}")

        else:
            output_file = os.path.join(
                self.output_directory, f"{self.carto_text_layer}.parquet"
            )
            try:
                gdf.to_parquet(output_file)
                self.logger.info(
                    f"Exported processed carto_text layer to {output_file}"
                )
            except Exception as e:
                self.logger.warning(f"Could not export to Parquet: {e}")

    def run(
        self,
        mapping_spreadsheet,
        full_layers_sheet,
        new_values_sheet,
        font_mapping_sheet,
        carto_text_folder,
        product_database,
        carto_text_layer,
        new_font_name,
        export_format="GPKG",
        create_csv_files=True,
    ):
        """
        Main orchestration method to run the complete processing workflow.
        """
        self.logger.info("=" * 80)
        self.logger.info("STARTING CARTO TEXT PROCESSING WORKFLOW")
        self.logger.info("=" * 80)

        # Validate inputs
        if not os.path.exists(mapping_spreadsheet):
            self.logger.error(f"File {mapping_spreadsheet} does not exist")
            return None

        # Process 'Full layers' sheet
        self.logger.info("Processing Full layers sheet...")
        full_layers_csv = self.process_fulllayers_tab(
            mapping_spreadsheet, full_layers_sheet
        )
        if full_layers_csv:
            self.logger.info(f"Full layers processing completed: {full_layers_csv}")

        # Process 'New values' sheet
        self.logger.info("Processing New values sheet...")
        new_values_csv = self.process_new_values_tab(
            mapping_spreadsheet, new_values_sheet
        )
        if new_values_csv:
            self.logger.info(f"New values processing completed: {new_values_csv}")

        # Process 'Font mapping' sheet
        self.logger.info("Processing Font mapping sheet...")
        font_mapping_csv = self.process_font_mapping_tab(
            mapping_spreadsheet, font_mapping_sheet
        )
        if font_mapping_csv:
            self.logger.info(f"Font mapping processing completed: {font_mapping_csv}")

        # Process carto_text layer with the CSV files
        self.logger.info("Processing carto_text layer...")
        carto_text_output_gdf = self.process_carto_text_layer(
            carto_text_folder,
            product_database,
            carto_text_layer,
            full_layers_csv,
            new_values_csv,
            font_mapping_csv,
            new_font_name,
        )

        # export the final result if processing was successful
        if carto_text_output_gdf is not None:
            self.logger.info("Exporting processed data...")
            self.export_data(carto_text_output_gdf, export_format=export_format)
            self.logger.info("Carto text layer processing completed successfully")
        else:
            self.logger.error("Carto text layer processing failed")

        if self.unmatched_ids:
            self.logger.warning(
                "The following layer_id and value combinations did not have matching font/style information and were set to null:"
            )
            for unmatched in self.unmatched_ids:
                self.logger.warning(f"  - {unmatched}")

        if self.multiple_value_rows:
            self.logger.warning(
                "The following layer_id and value combinations had multiple matching rows in new_values, which may require further review:"
            )
            for multiple in self.multiple_value_rows:
                self.logger.warning(
                    f"  - Layer ID: {multiple['layer_id']}, text_bend: {multiple['text_bend']}, text_height: {multiple['text_height']}, text_colour: {multiple['text_colour']}, placement: {multiple['placement']}"
                )
                self.logger.warning(f"    Matching rows: {multiple['matching_rows']}")

        self.logger.info("=" * 80)
        self.logger.info("CARTO TEXT PROCESSING WORKFLOW COMPLETED")
        self.logger.info("=" * 80)

        return carto_text_output_gdf


if __name__ == "__main__":
    # Configuration parameters
    # See carto_text_fields_readme.md for details on the pre & post set up instructions
    mapping_spreadsheet = (
        r"C:\Data\Topo50\Topo50_carto_text_2020_09\ratio-text-gap-to-twsd-latest.xlsx"
    )
    full_layers_sheet = "Full layers"
    new_values_sheet = "New values"
    font_mapping_sheet = "Font mapping"
    # output_directory = r"C:\temp\carto"
    output_directory = r"C:\Data\topoedit\topographic-product-data"
    logs_csv_folder = r"C:\temp\carto"

    carto_text_folder = r"C:\Data\toposource\topographic-product-data"
    product_database = "topographic-product-data.gpkg"
    carto_text_layer = "nz_topo50_carto_text"
    create_csv_files = True

    new_font_name = "Nimbus Sans"

    # Initialize processor and run workflow
    processor = CartoTextProcessor(
        output_directory, product_database, carto_text_layer, logs_csv_folder
    )

    result = processor.run(
        mapping_spreadsheet=mapping_spreadsheet,
        full_layers_sheet=full_layers_sheet,
        new_values_sheet=new_values_sheet,
        font_mapping_sheet=font_mapping_sheet,
        carto_text_folder=carto_text_folder,
        product_database=product_database,
        carto_text_layer=carto_text_layer,
        new_font_name=new_font_name,
        export_format="GPKG",
        create_csv_files=create_csv_files,
    )
