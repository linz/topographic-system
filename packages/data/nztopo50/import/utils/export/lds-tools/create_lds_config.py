import argparse
import json
from pathlib import Path

import pandas as pd

SCHEMA_KEY_ALIASES = {"linz_map_sheet": "NZ-Topo-Map-Sheet"}
ROAD_LDS_FIELDS = [
    ("t50_fid", "t50_fid"),
    ("name_ascii", "name_ascii"),
    ("macronated", "macronated"),
    ("name", "name"),
    ("hway_num", "highway_number"),
    ("rna_sufi", "rna_sufi"),
    ("lane_count", "lane_count"),
    ("way_count", "way_count"),
    ("status", "status"),
    ("surface", "surface"),
    ("geometry", "geometry"),
]


def load_field_mapping(mapping_file: Path) -> tuple[dict, dict]:
    mappings = pd.read_excel(mapping_file)
    field_names = {}
    mapped_names = {}
    for row in mappings.itertuples(index=False):
        field_names.setdefault(row.filename, []).append(row.field_name)
        mapped_names.setdefault(row.filename, []).append(row.mapped_name)

    field_names["road_cl"] = [field for field, _mapped in ROAD_LDS_FIELDS]
    mapped_names["road_cl"] = [mapped for _field, mapped in ROAD_LDS_FIELDS]
    return field_names, mapped_names


def create_configs(
    layer_info_file: Path,
    field_mapping_file: Path,
    schema_file: Path,
    output_directory: Path,
) -> tuple[int, list[str]]:
    layers = pd.read_excel(layer_info_file, sheet_name="Sheet1")
    with schema_file.open(encoding="utf-8") as file:
        schemas = json.load(file)

    field_names, mapped_names = load_field_mapping(field_mapping_file)

    configs = {}
    skipped_targets = []
    for row in layers.itertuples(index=False):
        target_layer = row.shp_name
        schema_key = SCHEMA_KEY_ALIASES.get(target_layer, target_layer)
        if schema_key not in schemas or schema_key not in field_names:
            skipped_targets.append(target_layer)
            continue

        schema = schemas[schema_key]
        source_by_target = dict(
            zip(field_names[schema_key], mapped_names[schema_key], strict=True)
        )
        target_fields = [*schema["properties"], "geometry"]
        missing_fields = [
            target_field
            for target_field in target_fields
            if target_field not in source_by_target
        ]
        if missing_fields:
            raise ValueError(
                f"LDS schema fields for {schema_key} have no source mapping: "
                f"{missing_fields}"
            )
        source_fields = [source_by_target[target_field] for target_field in target_fields]

        field_mappings = []
        for source_field, target_field in zip(
            source_fields, target_fields, strict=True
        ):
            target_type = (
                schema["geometry"]
                if target_field == "geometry"
                else schema["properties"][target_field]
            )
            field_mappings.append(
                {
                    "source_field": source_field,
                    "target_field": target_field,
                    "target_type": target_type,
                }
            )

        source_layer = row.layer_name.lower()
        config = configs.setdefault(
            source_layer,
            {
                "source_layer": source_layer,
                "targets": [],
            },
        )
        config["targets"].append(
            {
                "target_layer": target_layer,
                "schema_key": schema_key,
                "feature_type": row.type,
                "geometry_type": schema["geometry"],
                "field_mappings": field_mappings,
            }
        )

    output_directory.mkdir(parents=True, exist_ok=True)
    for source_layer, config in sorted(configs.items()):
        output_file = output_directory / f"{source_layer}.json"
        with output_file.open("w", encoding="utf-8") as file:
            json.dump(config, file, indent=2, ensure_ascii=False)
            file.write("\n")

    return len(configs), skipped_targets


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create one merged LDS mapping config per source model layer."
    )
    parser.add_argument("layer_info_file", type=Path)
    parser.add_argument("field_mapping_file", type=Path)
    parser.add_argument("schema_file", type=Path)
    parser.add_argument("output_directory", type=Path)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    config_count, skipped = create_configs(
        args.layer_info_file,
        args.field_mapping_file,
        args.schema_file,
        args.output_directory,
    )
    print(f"Created {config_count} source-layer configs")
    if skipped:
        print(f"Skipped targets without mappings/schemas: {', '.join(skipped)}")
