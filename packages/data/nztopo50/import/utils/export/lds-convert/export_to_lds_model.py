# This script exports the NZ Topo50 data model to LDS shapefiles.
# Per-source JSON configs define target layers, field mappings, and formats.

# NOTE: Currently only topographic data is supported.
import json
import logging
import os
from pathlib import Path

import duckdb
import geopandas as gpd  # type: ignore
import numpy as np
import pandas as pd
from export_layers_setup import ExportLayersSetup

"""
Utility functions and classes for exporting NZ Topo50 data to LDS shapefiles.
Coded against schema 0.3

Additional changes not implemented for later schema versions.
BM-1904 (done) - racetracks
1905- (done) cartotext, word_distance - not in export model
1906 - (done) added sub field ignore
1907 - (done) added sub field ignore
1909 - (done) main impact - require lookup for geographic_names
1914 - (done) track_line: move subtype to type and track_type to subtype
1915 - (done) trig_point: move trig_type to type - ignore not in schema
1916 - fields redesigned larger restructure - contour: use type for index, contour, supplementary and depression
1917 - (done) runway: move subtype to type and surface to subtype
1919 - (done) - residential area move
1928  (done)- rename landuse/cover
"""
# TODO on live version - need to deal with snow_poly - this is created from ice_poly but fid need to be grabbed from the origin snow_poly

# note: we null out 3 mainland research stations so question do we return them like tree names
#- { column: subtype, set: null, where: { t50_fid: 7862766, subtype: 'research station' } }
#- { column: subtype, set: null, where: { t50_fid: 7862767, subtype: 'research station' } }
#- { column: subtype, set: null, where: { t50_fid: 7862768, subtype: 'research station' } }

TYPE_SELECTION_ALIASES = {
    ("land_use", "racetrack"): ["racetrack", "horse_racetrack", "vehicle_racetrack"],
    ("structure_point", "beacon"): ["beacon", "lighthouse"],
}

TRACK_USE_BY_TYPE = {
    "racetrack": None,
    "horse_racetrack": "horse",
    "vehicle_racetrack": "vehicle",
    "cycle_racetrack": "cycle",
    "dog_racetrack": "dog",
}

DISPLAY_BY_SUBTYPE = {
    "large_boulder": 1,
    "small_rock_outcrop": 2,
    "large_rock_outcrop": 3,
}

BUILDING_STATUS_BY_FID = {
    159743: "closed",
    7798270: "closed",
    8411910: "locked",
}

BUILDING_SUBTYPE_BY_FID = {
    6744342: "tank",
    7862766: "research station",
    7862767: "research station",
    7862768: "research station",
}

DESC_CODE_BY_NAME = {
    "airport": "AIR",
    "bay_inlet": "BAY",
    "bay": "BAY",
    "inlet": "BAY",
    "beach": "BCH",
    "bridge": "BDGE",
    "building": "BLDG",
    "bank": "BNK",
    "mountain_basin": "BSN",
    "cave": "CAVE",
    "cemetery": "CEM",
    "camping_ground": "CGND",
    "cliff": "CLIF",
    "crater": "CRTR",
    "dam": "DAM",
    "estuary": "ESTY",
    "waterfall": "FALL",
    "farm": "FARM",
    "flat": "FLAT",
    "ford": "FORD",
    "forest": "FRST",
    "geothermal_area": "GEO",
    "glacier": "GLCR",
    "golf_links": "GLKS",
    "gorge_valley": "GORG",
    "gorge": "GORG",
    "valley": "GORG",
    "hill": "HILL",
    "historic": "HIST",
    "hospital": "HOSP",
    "homestead": "HSTD",
    "hut": "HUT",
    "island": "ISLD",
    "isthmus": "ISTH",
    "lake": "LAKE",
    "locality": "LOC",
    "lighthouse": "LTH",
    "miscellaneous_cultural_feature": "MCUL",
    "metropolis": "METR",
    "mine": "MINE",
    "miscellaneous_natural_feature": "MNAT",
    "miscellaneous_place": "MPLA",
    "marine_rock_formation": "MRFM",
    "navigational_beacon": "NAVB",
    "native_land_reserve": "NLR",
    "pa": "PA",
    "park": "PARK",
    "pass": "PASS",
    "peninsula": "PEN",
    "point": "PNT",
    "populated_place": "POPL",
    "pipeline": "PPLN",
    "racecourse": "RC",
    "ridge": "RDGE",
    "road_related_feature": "RDRF",
    "railway_related_feature": "RLYF",
    "range": "RNGE",
    "rock": "ROCK",
    "railway_station": "RSTN",
    "suburb": "SBRB",
    "school": "SCH",
    "sea": "SEA",
    "ski": "SKI",
    "slip": "SLIP",
    "spring": "SPR",
    "strait": "STRA",
    "stream": "STRM",
    "swamp": "SWMP",
    "town": "TOWN",
    "track": "TRCK",
    "tree": "TREE",
    "trig": "TRIG",
    "tunnel": "TUNL",
    "tramway": "TWAY",
    "university": "UNIV",
    "urban_satellite": "USAT",
    "well": "WELL",
    "wharf": "WHF",
    "wilderness_area": "WILD",
    "wreck": "WRCK",
    "water_way": "WWAY",
}

TUNNEL_SWAPPED_USE_FIDS = {6066307, 7745273, 8344640, 8499056, 8525734}
TUNNEL_IVESTOCK_FIDS = {6066314}


EXPORT_CRS = {
    "new_zealand_mainland": 2193,
    "chatham_islands": 3793,
    "auckland_islands": 3788,
    "snares_ak_islands": 3788,
    "antipodes_and_bounty_islands": 3790,
    "kermadec_raoul_islands": 3791,
    "campbell_island": 3789,
}

EXPORT_EXTENTS = {
    "new_zealand_mainland": [166, -47.54, 179.0, -33.9],
    "chatham_islands": [-177, -44.52, -176.131, -43.544],
    "snares_ak_islands": [166.4, -51.1, 166.7, -48.0],
    "auckland_islands": [165.7, -51.1, 166.4, -50.4],
    "antipodes_and_bounty_islands": [178.6, -49.9, 179.0, -49.5],
    "bounty_islands":[178.6, -47.9, 179.2, -47.6],
    "kermadec_raoul_islands": [-179, -31.42, -177.7, -29.15],
    "campbell_island": [168.9, -52.7, 169.3, -52.4],
}

ORIENTATION_LAYERS = {
    "building_point",
    "land_cover_point",
    "relief_point",
    "structure_point",
    "utility_point",
    "water_point",
}

SOURCE_CRS = "EPSG:4167"

class ExportToLDSModel:
    def __init__(
        self,
        config_directory,
        data_url,
        data_path_template,
        output_folder,
    ):
        self.config_directory = Path(config_directory)
        self.data_url = str(data_url).rstrip("/\\")
        self.data_path_template = data_path_template
        self.output_folder = output_folder
        os.makedirs(self.output_folder, exist_ok=True)
        self.connection = duckdb.connect()
        self.connection.execute("SET allow_asterisks_in_http_paths = true")
        try:
            self.connection.execute("LOAD spatial")
        except duckdb.Error:
            self.connection.execute("INSTALL spatial")
            self.connection.execute("LOAD spatial")
        self.logger = self.create_logger()
        self.layer_setup = ExportLayersSetup(self.config_directory)
        self.layers_info = self.layer_setup.layers_info
        self.layer_source_counts = self.layer_setup.layer_source_counts
        self.field_names = self.layer_setup.field_names
        self.mapped_names = self.layer_setup.mapped_names
        self.schemas = self.layer_setup.schemas

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
        logger.info(
            "LDS export started using GeoParquet source data at %s",
            self.data_url,
        )
        return logger

    def close_logger(self):
        for handler in self.logger.handlers[:]:
            handler.flush()
            handler.close()
            self.logger.removeHandler(handler)
        self.connection.close()

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

    @staticmethod
    def get_desc_code(name):
        lookup = DESC_CODE_BY_NAME.get(name)
        return lookup

    def parquet_path(self, layer_name):
        try:
            relative_path = self.data_path_template.format(layer_name=layer_name)
        except KeyError as error:
            raise ValueError(
                f"Unsupported placeholder {error.args[0]!r} in data_path"
            ) from error
        if "{layer_name}" not in self.data_path_template:
            raise ValueError("data_path must contain the {layer_name} placeholder")
        separator = "/" if "://" in self.data_url else os.sep
        return f"{self.data_url}{separator}{relative_path.lstrip('/\\')}"

    def read_source_data(self, layer_name, feature_type, area_name):
        bbox = EXPORT_EXTENTS[area_name]
        selection_types = TYPE_SELECTION_ALIASES.get(
            (layer_name, feature_type), [feature_type]
        )
        west, south, east, north = bbox
        query = """
            SELECT * EXCLUDE (bbox, geometry), ST_AsWKB(geometry) AS geometry
            FROM read_parquet(?)
            WHERE bbox.xmax >= ?
              AND bbox.xmin <= ?
              AND bbox.ymax >= ?
              AND bbox.ymin <= ?
                AND ST_Intersects(
                        geometry,
                        ST_MakeEnvelope(?, ?, ?, ?)
                )
        """
        parameters = [
            self.parquet_path(layer_name),
            west,
            east,
            south,
            north,
            west,
            south,
            east,
            north,
        ]
        if self.layer_source_counts.get(layer_name, 0) > 1:
            query += f" AND type IN ({', '.join('?' for _ in selection_types)})"
            parameters.extend(selection_types)

        start_time = pd.Timestamp.now()
        start_message = f"Query start: {layer_name} {start_time}"
        print(start_message)
        self.logger.info(start_message)
        frame = self.connection.execute(query, parameters).fetchdf()
        end_time = pd.Timestamp.now()
        end_message = (
            f"Query end: {layer_name} {end_time}; "
            f"delta: {end_time - start_time}"
        )
        print(end_message)
        self.logger.info(end_message)
        geometry = frame.pop("geometry").map(
            lambda value: bytes(value) if value is not None else None
        )
        return gpd.GeoDataFrame(
            frame,
            geometry=gpd.GeoSeries.from_wkb(geometry, crs=SOURCE_CRS),
        )

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

            for t50_fid, status in BUILDING_STATUS_BY_FID.items():
                normalized = layer["t50_fid"].eq(t50_fid) & layer["status"].isna()
                layer.loc[normalized, "status"] = status

            for t50_fid, subtype in BUILDING_SUBTYPE_BY_FID.items():
                normalized = layer["t50_fid"].eq(t50_fid) & layer["subtype"].isna()
                layer.loc[normalized, "subtype"] = subtype

            layer["building_use"] = layer["subtype"]
        elif layer_name == "geographic_name":
            layer["type"] = layer["type"].map(self.get_desc_code)
        elif layer_name == "bridge_line":
            layer["bridge_use"] = layer["type"]
            layer["bridge_use2"] = layer["subtype"]
        elif layer_name == "land_cover_point":
            layer["display"] = layer["subtype"].map(DISPLAY_BY_SUBTYPE)
        elif layer_name in {"land_use", "land_use_line"}:
            layer["landuse_type"] = layer["subtype"]
            layer["type"] = layer["type"].replace(TRACK_USE_BY_TYPE)
        elif layer_name == "land_use_point":
            layer["place_type"] = layer["subtype"]
            if feature_type == "mine":
                layer["status"] = layer["status"].replace({"old": "historic"})
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
            layer["rna_sufi"] = layer["rna_sufi"].mask(layer["rna_sufi"] == 0)
        elif layer_name == "runway":
            layer["runway_use"] = layer["subtype"]
            layer["subtype"] = layer["subtype"].where(layer["subtype"] != "grass")
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
            swapped_uses = layer["t50_fid"].isin(TUNNEL_SWAPPED_USE_FIDS)
            layer.loc[swapped_uses, "type"] = "livestock"
            layer.loc[swapped_uses, "subtype"] = "vehicle"
            layer["type"] = layer["type"].replace(
                {"foot_traffic": "foot traffic"}
            )
            ivestock_uses = layer["t50_fid"].isin(TUNNEL_IVESTOCK_FIDS)
            layer.loc[ivestock_uses, "subtype"] = "ivestock"
        elif layer_name == "utility_line" and feature_type == "pipeline":
            layer["infrastructure_use"] = layer["subtype"]
        elif layer_name == "vegetation":
            if feature_type == "exotic":
                layer["subtype"] = layer["subtype"].where(
                    layer["subtype"] != "coniferous"
                )
            layer["species"] = layer["subtype"]
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
        message = "Checking for macronated names..."
        print(message)
        self.logger.info(message)
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
                    lambda value: (
                        None
                        if pd.isna(value)
                        or (isinstance(value, str) and value == "<NA>")
                        else value.replace("_", " ")
                        if isinstance(value, str)
                        else value
                    )
                )
        return layer

    @staticmethod
    def restore_orientation_radians(layer, layer_name):
        if layer_name in ORIENTATION_LAYERS:
            degrees = pd.to_numeric(layer["orientation"], errors="coerce")
            layer["orientation"] = np.deg2rad(-degrees)
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

    def process_layers(self, area_name, single_file=None):
        processed_layers = []
        try:
            layer_items = self.layer_setup.get_layer_items(
                extent_name=area_name,
                single_file=single_file,
            )
        except ValueError as error:
            self.logger.error(str(error))
            self.close_logger()
            raise

        if single_file:
            self.logger.info("Processing only %s", Path(single_file).stem.lower())

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

            try:
                layer = self.read_source_data(
                    layer_name,
                    feature_type,
                    area_name,
                )
            except Exception:
                message = (
                    f"Unable to open {self.parquet_path(layer_name)}, "
                    f"layer {layer_name}, "
                    f"type {feature_type}; skipping {shp_name}"
                )
                print(message)
                self.logger.exception(message)
                continue

            if layer.empty:
                message = (
                    f"No features found for {layer_name}, "
                    f"type {feature_type}, area {area_name}; skipping {shp_name}"
                )
                print(message)
                self.logger.info(message)
                continue

            layer = self.restore_orientation_radians(layer, layer_name)
            layer = self.restore_source_fields(layer, layer_name, feature_type)

            if "name" in mapped_names or feature_type == "historic_site":
                layer = self.check_names(mapped_names, layer, feature_type)

            if feature_type == "tree":
                layer = self.tree_locations_special_case(layer)
            layer = self.reorder_columns(layer, mapped_names, field_names)
            layer.to_crs(EXPORT_CRS[area_name], inplace=True)
            schema = self.read_schema_layer(shp_name)
            layer = self.restore_lds_string_values(layer, schema)
            layer.to_file(shp_path, engine="fiona", schema=schema, encoding="UTF-8")
            processed_layers.append(feature_type)
            end_time = pd.Timestamp.now()
            message = (
                f"Exported: {layer_name} of type {feature_type} to {shp_name}: "
                f"{end_time - start_time}"
            )
            print(message)
            self.logger.info(message)

            if feature_type == "ice":
                shp_path = shp_path.replace("ice", "snow")
                layer.to_file(shp_path, engine="fiona", schema=schema, encoding="UTF-8")
                print(f"Exported: {layer_name} to {shp_path}")
                self.logger.info("Exported %s to %s", layer_name, shp_path)

        print(f"Processed layers: {len(processed_layers)}")
        self.logger.info("LDS export finished; processed %s layers", len(processed_layers))
        self.close_logger()

