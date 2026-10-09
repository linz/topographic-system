# This script exports the NZ Topo50 data model to LDS shapefiles.
# Per-source JSON configs define target layers, field mappings, and formats.
import json
import logging
import os
from pathlib import Path

import geopandas as gpd  # type: ignore
import pandas as pd

TYPE_SELECTION_ALIASES = {
    ("landuse", "racetrack"): ["racetrack", "horse_track", "vehicle_track"],
    ("structure_point", "beacon"): ["beacon", "lighthouse"],
}

TRACK_USE_BY_TYPE = {
    "horse_track": "horse",
    "vehicle_track": "vehicle",
    "cycle_track": "cycle",
    "dog_track": "dog",
}

DISPLAY_BY_SUBTYPE = {
    "large_boulder": "1",
    "small_rock_outcrop": "2",
    "large_rock_outcrop": "3",
}

class ExportToLDSModel:
    def __init__(
        self,
        config_directory,
        database,
        contour_database,
        product_database,
        output_folder,
        source_format="gpkg",
    ):
        self.config_directory = Path(config_directory)
        self.database = database
        self.contour_database = contour_database
        self.product_database = product_database
        self.output_folder = output_folder
        self.source_format = source_format.lower()
        if self.source_format not in {"gpkg", "parquet"}:
            raise ValueError(f"Unsupported source format: {source_format}")
        os.makedirs(self.output_folder, exist_ok=True)
        self.logger = self.create_logger()
        (
            self.layers_info,
            self.layer_source_counts,
            self.field_names,
            self.mapped_names,
            self.schemas,
        ) = self.load_configs(self.config_directory)

    @staticmethod
    def create_fiona_schema(target_config):
        properties = {}
        geometry_mapping_count = 0
        for field_mapping in target_config["field_mappings"]:
            target_field = field_mapping["target_field"]
            target_type = field_mapping["target_type"]
            if target_field == "geometry":
                geometry_mapping_count += 1
                if target_type != target_config["geometry_type"]:
                    raise ValueError(
                        f"Geometry mapping for {target_config['target_layer']} "
                        "does not match geometry_type"
                    )
                continue
            if target_field in properties:
                raise ValueError(
                    f"Duplicate target field {target_field} in "
                    f"{target_config['target_layer']}"
                )
            properties[target_field] = target_type

        if geometry_mapping_count != 1:
            raise ValueError(
                f"Expected one geometry mapping for "
                f"{target_config['target_layer']}, found {geometry_mapping_count}"
            )
        return {
            "properties": properties,
            "geometry": target_config["geometry_type"],
        }

    def load_configs(self, config_directory):
        config_files = sorted(config_directory.glob("*.json"))
        if not config_files:
            raise ValueError(f"No LDS config files found in {config_directory}")

        layers_info = {}
        layer_source_counts = {}
        field_names = {}
        mapped_names = {}
        schemas = {}

        for config_file in config_files:
            with config_file.open(encoding="utf-8") as file:
                config = json.load(file)
            source_layer = config["source_layer"].lower()
            if config_file.stem.lower() != source_layer:
                raise ValueError(
                    f"Config filename {config_file.name} does not match "
                    f"source_layer {source_layer}"
                )

            targets = config["targets"]
            layer_source_counts[source_layer] = len(targets)
            for target in targets:
                target_layer = target["target_layer"]
                if target_layer in layers_info:
                    raise ValueError(f"Duplicate LDS target layer: {target_layer}")

                layers_info[target_layer] = [target["feature_type"], source_layer]
                field_names[target_layer] = [
                    mapping["target_field"] for mapping in target["field_mappings"]
                ]
                mapped_names[target_layer] = [
                    mapping["source_field"] for mapping in target["field_mappings"]
                ]
                schemas[target_layer] = self.create_fiona_schema(target)

        return (
            layers_info,
            layer_source_counts,
            field_names,
            mapped_names,
            schemas,
        )

    def create_logger(self):
        logger = logging.getLogger(f"{__name__}.{id(self)}")
        logger.setLevel(logging.INFO)
        logger.propagate = False
        handler = logging.FileHandler(
            os.path.join(self.output_folder, "export_to_lds_model.log"),
            mode="w",
            encoding="utf-8",
        )
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        )
        logger.addHandler(handler)
        logger.info("LDS export started using %s source data", self.source_format)
        return logger

    def close_logger(self):
        for handler in self.logger.handlers[:]:
            handler.flush()
            handler.close()
            self.logger.removeHandler(handler)

    def is_macronated(self, word):
        macronated_maori_vowels = {"ā", "ē", "ī", "ō", "ū", "è"}
        return any(char.lower() in macronated_maori_vowels for char in word)

    def language_test(self, word):
        macronateds = {
            "à",
            "á",
            "â",
            "ã",
            "ä",
            "è",
            "é",
            "ê",
            "ë",
            "ì",
            "í",
            "î",
            "ï",
            "ò",
            "ó",
            "ô",
            "õ",
            "ö",
            "ù",
            "ú",
            "û",
            "ü",
            "ç",
            "ñ",
            "ß",
        }
        return any(char.lower() in macronateds for char in word)

    def remove_macrons(self, text):
        macron_map = {
            "ā": "a",
            "ē": "e",
            "ī": "i",
            "ō": "o",
            "ū": "u",
            "è": "e",
            "Ā": "A",
            "Ē": "E",
            "Ī": "I",
            "Ō": "O",
            "Ū": "U",
            "È": "E",
        }
        return "".join(macron_map.get(char, char) for char in text)

    def read_source_data(self, format, layer_name, feature_type, database=None):
        database = database or self.database
        selection_types = TYPE_SELECTION_ALIASES.get(
            (layer_name, feature_type), [feature_type]
        )
        where = None
        if self.layer_source_counts.get(layer_name, 0) > 1:
            quoted_types = ", ".join(
                f"'{value.replace(chr(39), chr(39) * 2)}'"
                for value in selection_types
            )
            where = f'"type" IN ({quoted_types})'

        if format.lower() == "gpkg":
            layer = gpd.read_file(
                database,
                layer=layer_name,
                where=where,
            )
        elif format.lower() == "parquet":
            parquet_file = os.path.join(database, f"{layer_name}.parquet")
            layer = gpd.read_parquet(parquet_file)
            if where is not None:
                layer = layer[layer["type"].isin(selection_types)]
        else:
            raise ValueError(f"Unsupported format: {format}")
        return layer

    def read_schema_layer(self, layer_name):
        schema = self.schemas.get(layer_name)
        if schema is None:
            raise KeyError(f"No LDS schema found for {layer_name}")
        return schema

    def metadata_source_key(self, metadata, key_name):
        if metadata is None or metadata is pd.NA:
            return None
        if isinstance(metadata, bytes):
            metadata = metadata.decode("utf-8")
        if isinstance(metadata, str):
            if not metadata.strip():
                return None
            try:
                metadata = json.loads(metadata)
            except (json.JSONDecodeError, TypeError):
                return None
        if isinstance(metadata, dict):
            metadata = [metadata]
        if not isinstance(metadata, (list, tuple)):
            return None
        for item in metadata:
            if isinstance(item, dict) and item.get("source_key_name") == key_name:
                return item.get("source_key_value")
        return None

    def restore_source_fields(self, layer, layer_name, feature_type):
        """Recreate source columns removed by model normalization."""
        layer = layer.copy()

        if "substance_extracted" in layer:
            layer["substance"] = layer["substance_extracted"]

        if layer_name in {"building", "building_point"}:
            layer["building_use"] = layer["subtype"]
        elif layer_name == "bridge_line":
            layer["bridge_use"] = layer["type"]
            layer["bridge_use2"] = layer["subtype"]
        elif layer_name == "landcover_point":
            layer["display"] = layer["subtype"].map(DISPLAY_BY_SUBTYPE)
        elif layer_name in {"landuse", "landuse_line"}:
            layer["landuse_type"] = layer["subtype"]
            layer["landuse_use"] = layer["type"].map(TRACK_USE_BY_TYPE)
        elif layer_name == "landuse_point":
            layer["place_type"] = layer["subtype"]
        elif layer_name in {"marine", "marine_point"}:
            layer["composition"] = layer["subtype"]
        elif layer_name == "place_point":
            layer["composition"] = layer["subtype"]
            if feature_type == "historic_site":
                layer["description"] = layer["name"]
        elif layer_name == "railway_line":
            layer["railway_use"] = layer["subtype"]
            layer["vehicle_type"] = layer["vehicle_type"].where(
                layer["vehicle_type"] != "train"
            )
        elif layer_name == "relief_line" and feature_type == "embankment":
            layer["relief_use"] = layer["subtype"]
        elif layer_name == "road_line" and "metadata" in layer:
            layer["rna_sufi"] = pd.to_numeric(
                layer["metadata"].map(
                    lambda value: self.metadata_source_key(value, "road_id")
                ),
                errors="coerce",
            )
        elif layer_name == "runway":
            layer["runway_use"] = layer["subtype"]
            layer["surface"] = layer["surface"].where(layer["surface"] != "grass")
        elif layer_name == "structure":
            if feature_type == "tank":
                layer["structure_type"] = layer["tank_type"]
                layer["stored_item"] = layer["subtype"]
            else:
                layer["structure_type"] = layer["subtype"]
        elif layer_name == "structure_line":
            if feature_type == "cableway_industrial":
                layer["material_conveyed"] = layer["subtype"]
            elif feature_type == "cableway_people":
                layer["restrictions"] = layer["subtype"]
            elif feature_type == "wharf":
                layer["structure_use"] = layer["subtype"]
        elif layer_name == "structure_point":
            if feature_type == "beacon":
                layer["structure_type"] = layer["type"].where(
                    layer["type"] == "lighthouse"
                )
            elif feature_type == "bivouac":
                layer["material"] = layer["subtype"].where(
                    layer["subtype"] != "building"
                )
            elif feature_type == "tower":
                layer["material"] = layer["subtype"]
            elif feature_type == "gate":
                layer["restrictions"] = layer["subtype"]
            elif feature_type in {"shaft", "windmill"}:
                layer["structure_use"] = layer["subtype"]
            elif feature_type == "tank":
                layer["stored_item"] = layer["subtype"]
                layer["structure_type"] = layer["tank_type"]
            elif feature_type == "wreck":
                layer["wreck_of"] = layer["subtype"]
        elif layer_name == "trig_point":
            layer["name"] = layer["code"]
        elif layer_name == "tunnel_line":
            swapped_uses = (layer["type"] == "vehicle") & (
                layer["subtype"] == "livestock"
            )
            layer.loc[swapped_uses, "type"] = "livestock"
            layer.loc[swapped_uses, "subtype"] = "vehicle"
            layer["type"] = layer["type"].replace(
                {"foot_traffic": "foot traffic"}
            )
            layer["subtype"] = layer["subtype"].replace(
                {"livestock": "ivestock"}
            )
        elif layer_name == "track_line":
            layer["track_use"] = layer["subtype"]
        elif layer_name == "utility_line" and feature_type == "pipeline":
            layer["infrastructure_use"] = layer["subtype"]
        elif layer_name == "vegetation":
            layer["species"] = layer["subtype"]
            if feature_type == "exotic":
                layer["species"] = layer["species"].where(
                    layer["species"] != "coniferous"
                )
        elif layer_name == "water":
            layer["water_use"] = layer["subtype"]
            if "temperature_indicator" in layer:
                layer["temperature"] = layer["temperature_indicator"]

        return layer

    def check_names(self, mapped_names, layer, feature_type):
        # historic_site special case - only has description field. In reality all current historic_site names are not macronated = (N)
        # tree_pnt we droppped the name field so no need to check
        if feature_type == "tree":
            return layer
        print("Checking for macronated names...")
        layer["macronated"] = "N"
        name_field = "description"
        if feature_type != "historic_site":
            name_field = "name"
            layer["name_ascii"] = layer["name"]

        if "group_name" in mapped_names:
            layer["grp_macron"] = "N"
            layer["grp_ascii"] = layer["group_name"]

        for idx, row in layer.iterrows():
            name_value = row[name_field]
            # test check other languages
            language_test_result = self.language_test(str(name_value))
            if language_test_result:
                print(f"Language test for {name_value}: {language_test_result}")
            if name_value and self.is_macronated(str(name_value)):
                # print(f"Macronated name found: {name_value}")
                layer.at[idx, "macronated"] = "Y"
                if feature_type != "historic_site":
                    layer.at[idx, "name_ascii"] = self.remove_macrons(name_value)

            if "group_name" in mapped_names:
                group_value = row["group_name"]
                if group_value and self.is_macronated(str(group_value)):
                    # print(f"Macronated group name found: {group_value}")
                    layer.at[idx, "grp_macron"] = "Y"
                    layer.at[idx, "grp_ascii"] = self.remove_macrons(group_value)

        return layer

    def reorder_columns(self, layer, mapped_names, field_names):
        # Removed source fields that cannot be reconstructed are emitted as null.
        for mapped_name in mapped_names:
            if mapped_name != "geometry" and mapped_name not in layer.columns:
                layer[mapped_name] = None

        # Drop fields not in mapped_names and rename to field_names
        # Keep only fields that are in mapped_names
        fields_to_keep = [
            col for col in layer.columns if col in mapped_names or col == "geometry"
        ]
        layer = layer[fields_to_keep]

        # Create rename mapping from mapped_names to field_names
        rename_mapping = {}
        for i, mapped_name in enumerate(mapped_names):
            if (
                mapped_name in layer.columns
                and i < len(field_names)
                and mapped_name != field_names[i]
            ):
                rename_mapping[mapped_name] = field_names[i]

        # Apply renaming
        if rename_mapping:
            layer = layer.rename(columns=rename_mapping)

        # Reorder columns based on field_names order
        if field_names:
            # Get geometry column separately
            geometry_col = (
                layer.geometry.name if hasattr(layer, "geometry") else "geometry"
            )

            # Create ordered column list: field_names + geometry
            ordered_columns = []
            for field_name in field_names:
                if field_name in layer.columns:
                    ordered_columns.append(field_name)

            # Add geometry column if it exists and isn't already included
            if geometry_col in layer.columns and geometry_col not in ordered_columns:
                ordered_columns.append(geometry_col)

            # Reorder the dataframe
            layer = layer[ordered_columns]

        return layer

    def restore_lds_string_values(self, layer, schema):
        layer = layer.copy()
        if "lake_use" in layer.columns:
            layer["lake_use"] = layer["lake_use"].replace(
                {"hydro_electric": "hydro-electric"}
            )
        for field_name, field_type in schema["properties"].items():
            if field_type.startswith("str") and field_name in layer.columns:
                layer[field_name] = layer[field_name].map(
                    lambda value: value.replace("_", " ")
                    if isinstance(value, str)
                    else value
                )
        return layer

    def tree_locations_special_case(self, layer):
        layer["macronated"] = "N"
        layer["name"] = ""
        layer["name_ascii"] = ""

        # Special case for specific t50_fid values to set name fields
        fids = [4868150, 6083285, 6083286]
        for fid in fids:
            mask = layer["t50_fid"] == fid
            if mask.any():
                layer.loc[mask, "name"] = "Takarunga/Mount Victoria"
                layer.loc[mask, "name_ascii"] = "Takarunga/Mount Victoria"

        # Tuahu Kauri Tree 4731396
        mask = layer["t50_fid"] == 4731396
        if mask.any():
            layer.loc[mask, "name"] = "Tuahu Kauri Tree"
            layer.loc[mask, "name_ascii"] = "Tuahu Kauri Tree"

        # Waihi Golf Course 4875366
        mask = layer["t50_fid"] == 4875366
        if mask.any():
            layer.loc[mask, "name"] = "Waihi Golf Course"
            layer.loc[mask, "name_ascii"] = "Waihi Golf Course"

        # Weatherall's Trees 3703133, 3703134, 3703135
        fids = [3703133, 3703134, 3703135]
        for fid in fids:
            mask = layer["t50_fid"] == fid
            if mask.any():
                layer.loc[mask, "name"] = "Weatherall's Trees"
                layer.loc[mask, "name_ascii"] = "Weatherall's Trees"

        return layer

    def skip_unknown_nodata_layers(self, shape_name):
        # in layers_info but does not exist in data - keep skipping
        skip = {
            "blowhole_pnt",
            "cattlestop_pnt",
            "fume_cl",
            "flume_pnt",
            "kiln_pnt",
            "plantation_poly",
        }
        return shape_name in skip

    def process_layers(self, single_file=None):
        processed_layers = []
        layer_items = self.layers_info.items()
        if single_file:
            selected_name = os.path.splitext(os.path.basename(single_file))[0].lower()
            layer_items = [
                (shp_name, layer_info)
                for shp_name, layer_info in layer_items
                if shp_name.lower() == selected_name
            ]
            if not layer_items:
                message = f"Unknown LDS output file: {single_file}"
                self.logger.error(message)
                self.close_logger()
                raise ValueError(message)
            self.logger.info("Processing only %s", selected_name)

        for shp_name, layer_info in layer_items:
            start_time = pd.Timestamp.now()
            feature_type, layer_name = layer_info

            if self.skip_unknown_nodata_layers(shp_name):
                message = f"Skipping unknown/no data layer: {shp_name}"
                print(message)
                self.logger.warning(message)
                continue

            layer_name = layer_name.lower()
            shp_path = os.path.join(self.output_folder, f"{shp_name}.shp").lower()
            field_names = self.field_names.get(shp_name, [])
            mapped_names = self.mapped_names.get(shp_name, [])

            if layer_name == "contour" and os.path.exists(self.contour_database):
                database = self.contour_database
            elif layer_name in ["nz_topo50_map_sheet"] and os.path.exists(
                self.product_database
            ):
                database = self.product_database
            else:
                database = self.database

            try:
                layer = self.read_source_data(
                    self.source_format, layer_name, feature_type, database=database
                )
            except Exception:
                message = (
                    f"Unable to open {database}, layer {layer_name}, "
                    f"type {feature_type}; skipping {shp_name}"
                )
                print(message)
                self.logger.exception(message)
                continue

            layer = self.restore_source_fields(layer, layer_name, feature_type)
            if "name" in mapped_names or feature_type == "historic_site":
                layer = self.check_names(mapped_names, layer, feature_type)

            if feature_type == "tree":
                layer = self.tree_locations_special_case(layer)
            layer = self.reorder_columns(layer, mapped_names, field_names)
            layer.to_crs(2193, inplace=True)
            schema = self.read_schema_layer(shp_name)
            layer = self.restore_lds_string_values(layer, schema)
            layer.to_file(shp_path, engine="fiona", schema=schema, encoding="UTF-8")
            processed_layers.append(feature_type)
            end_time = pd.Timestamp.now()
            print(
                f"Exported: {layer_name} of type {feature_type} to {shp_name}: {end_time - start_time}"
            )
            self.logger.info(
                "Exported %s type %s to %s in %s",
                layer_name,
                feature_type,
                shp_name,
                end_time - start_time,
            )

            if feature_type == "ice":
                shp_path = shp_path.replace("ice", "snow")
                layer.to_file(shp_path, engine="fiona", schema=schema, encoding="UTF-8")
                print(f"Exported: {layer_name} to {shp_path}")
                self.logger.info("Exported %s to %s", layer_name, shp_path)

        print(f"Processed layers: {len(processed_layers)}")
        self.logger.info("LDS export finished; processed %s layers", len(processed_layers))
        self.close_logger()


if __name__ == "__main__":
    script_folder = os.path.dirname(os.path.abspath(__file__))
    config_directory = os.path.join(script_folder, "topo50_schemas", "config")

    # For GeoPackage sources, use source_format="gpkg" and set each value to
    # its .gpkg file. For GeoParquet, use source_format="parquet" and set each
    # value to a folder containing one <model_layer_name>.parquet file per layer.
    source_format = "parquet"
    database = r"C:\Data\temp\topographic-data-dev"
    contour_database = r"C:\Data\temp\topographic-contour-data"
    product_database = r"C:\Data\temp\topographic-product-data"

    # Set to an LDS output name such as "road_cl" or "road_cl.shp".
    # Use None to process every configured output file.
    single_file = None

    output_folder = r"C:\Data\topo50\export\lds_model"
    output_folder = r"C:\temp\export\lds_model"

    os.makedirs(output_folder, exist_ok=True)

    start_time = pd.Timestamp.now()
    exporter = ExportToLDSModel(
        config_directory,
        database,
        contour_database,
        product_database,
        output_folder,
        source_format=source_format,
    )
    exporter.process_layers(single_file=single_file)
    end_time = pd.Timestamp.now()
    print(f"Processing time: {end_time - start_time}")
