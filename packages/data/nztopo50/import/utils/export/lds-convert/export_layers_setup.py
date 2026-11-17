import json
from pathlib import Path


class ExportLayersSetup:
    def __init__(self, config_directory):
        (
            self.layers_info,
            self.layer_source_counts,
            self.field_names,
            self.mapped_names,
            self.schemas,
        ) = self.load_configs(Path(config_directory))
        self.data_sources = self.load_data_sources(
            Path(config_directory).parent / "data_sources.json"
        )

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

    @staticmethod
    def load_data_sources(data_sources_file):
        if not data_sources_file.exists():
            raise ValueError(f"No data source configuration found at {data_sources_file}")
        with data_sources_file.open(encoding="utf-8") as file:
            return json.load(file)

    def get_layer_items(self, extent_name, single_file=None):
        extent_layers = self.data_sources.get(extent_name)
        if extent_layers is None:
            raise ValueError(f"Unknown export extent: {extent_name}")

        source_layers = set(extent_layers)
        layer_items = list(self.layers_info.items())
        if single_file:
            selected_name = Path(single_file).stem.lower()
            layer_items = [
                (shp_name, layer_info)
                for shp_name, layer_info in layer_items
                if shp_name.lower() == selected_name
            ]
            if not layer_items:
                raise ValueError(f"Unknown LDS output file: {single_file}")

        layer_items = [
            (shp_name, layer_info)
            for shp_name, layer_info in layer_items
            if layer_info[1] in source_layers
        ]

        return layer_items
