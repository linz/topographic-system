import json
from datetime import datetime
from pathlib import Path
import pandas as pd
import pyogrio
import fastjsonschema
from fastjsonschema import JsonSchemaException

gpkg_path = Path(r"C:\Data\toposource\topographic-data\topographic-data")
if gpkg_path.suffix.lower() != ".gpkg":
    gpkg_path = gpkg_path.with_suffix(".gpkg")

schema_dir = Path(r"C:\Data\toposource\schema_model")

error_log_path = Path("val_errors.log")
error_log_path.write_text("", encoding="utf-8")


def write_error_log(lines):
    with error_log_path.open("a", encoding="utf-8") as log_file:
        for line in lines:
            log_file.write(f"{line}\n")


def normalize_name(name):
    normalized = str(name).replace("\\", "/").strip()
    if normalized.lower().startswith("next/"):
        normalized = normalized[5:]
    return normalized


def normalize_next_uri(value):
    if not isinstance(value, str):
        return value

    # Collapse repeated next/ prefixes and convert to a stable absolute URI.
    normalized = value.replace("\\", "/").strip()
    while normalized.lower().startswith("next/"):
        normalized = normalized[5:]
    return f"https://schemas.next.local/{normalized}"


def sanitize_schema_uris(value):
    if isinstance(value, dict):
        sanitized = {}
        for key, item in value.items():
            if key == "$id":
                sanitized[key] = normalize_next_uri(item)
            elif key == "$ref" and isinstance(item, str) and item.lower().startswith("next/"):
                sanitized[key] = normalize_next_uri(item)
            else:
                sanitized[key] = sanitize_schema_uris(item)
        return sanitized

    if isinstance(value, list):
        return [sanitize_schema_uris(item) for item in value]

    return value

# ------------------------------------------------------------------
# Validate each feature
# ------------------------------------------------------------------


def normalize_json_value(value):
    if value is None:
        return None

    if pd.isna(value):
        return None

    # Convert pandas/python datetimes to RFC3339 UTC strings.
    if isinstance(value, (pd.Timestamp, datetime)):
        value_utc = pd.to_datetime(value, errors="coerce", utc=True)
        if pd.notna(value_utc):
            return value_utc.isoformat().replace("+00:00", "Z")
        return None

    # Convert numpy scalar types (int64, float64, etc.) to Python scalars.
    if hasattr(value, "item"):
        try:
            return normalize_json_value(value.item())
        except (ValueError, TypeError):
            pass

    if isinstance(value, dict):
        return {k: normalize_json_value(v) for k, v in value.items()}

    if isinstance(value, list):
        return [normalize_json_value(v) for v in value]

    return value


def validate_metadata_item(item):
    required = [
        "table_column",
        "source",
        "source_key_name",
        "source_key_value",
        "source_table",
        "source_column",
        "source_updated_at",
        "imported_at",
    ]

    if not isinstance(item, dict):
        return "metadata item must be an object", item

    extra_keys = set(item.keys()) - set(required)
    if extra_keys:
        return f"additional properties are not allowed: {sorted(extra_keys)}", item

    missing_keys = [k for k in required if k not in item]
    if missing_keys:
        return f"missing required keys: {missing_keys}", item

    for key in ["table_column", "source", "source_key_name", "source_table", "source_column", "source_updated_at", "imported_at"]:
        if not isinstance(item[key], str):
            return f"{key} must be a string", item[key]

    if item["source"] not in ["nzgb_gazetteer", "linz_aims"]:
        return "source must be one of ['nzgb_gazetteer', 'linz_aims']", item["source"]

    key_value = item["source_key_value"]
    is_valid_int = isinstance(key_value, int) and 0 <= key_value <= 4294967295
    if not (is_valid_int or isinstance(key_value, str)):
        return "source_key_value must be an integer (0..4294967295) or string", key_value

    for key in ["source_updated_at", "imported_at"]:
        parsed = pd.to_datetime(item[key], errors="coerce", utc=True)
        if pd.isna(parsed):
            return f"{key} must be a valid date-time", item[key]

    return None, None

error_count = 0
max_rows_per_path = 10
path_fail_counts = {}
suppressed_paths = set()

if not gpkg_path.exists():
    raise FileNotFoundError(f"GeoPackage not found: {gpkg_path}")
if not schema_dir.exists():
    raise FileNotFoundError(f"Schema directory not found: {schema_dir}")

layer_info = pyogrio.list_layers(gpkg_path)
layers = [item[0] if not isinstance(item, str) else item for item in layer_info]

schema_map = {}
for schema_file in schema_dir.rglob("*.json"):
    rel_no_ext = schema_file.relative_to(schema_dir).with_suffix("")
    schema_key = normalize_name(rel_no_ext)
    if schema_key not in schema_map:
        schema_map[schema_key] = schema_file

print(f"Found {len(layers)} layer(s) in {gpkg_path.name}")

layers_validated = 0
layers_skipped = 0
layer_error_counts = {}

for layer in layers:
    layer_key = normalize_name(layer)
    schema_path = schema_map.get(layer_key)

    if schema_path is None:
        layers_skipped += 1
        print(f"\nSkipping layer '{layer}': schema '{layer_key}.json' not found")
        continue

    with schema_path.open("r", encoding="utf-8") as f:
        schema = sanitize_schema_uris(json.load(f))
    validate = fastjsonschema.compile(schema)

    gdf = pyogrio.read_dataframe(gpkg_path, layer=layer)

    # Ensure datetime columns are JSON/schema friendly before validation.
    for col in ["created_at", "updated_at"]:
        if col in gdf.columns:
            # JSON Schema date-time expects RFC3339, including timezone offset.
            col_utc = pd.to_datetime(gdf[col], errors="coerce", utc=True)
            gdf[col] = col_utc.apply(
                lambda value: value.isoformat().replace("+00:00", "Z") if pd.notna(value) else None
            )

    print(f"\nValidating layer '{layer}' ({len(gdf)} feature(s))")

    layer_errors_before = error_count

    for idx, row in gdf.iterrows():

        # Convert row to dictionary, normalizing pandas missing values to JSON null.
        feature = normalize_json_value(row.where(pd.notna(row), None).to_dict())

        # Metadata can be stored as JSON text; parse to list/object for schema validation.
        metadata_value = feature.get("metadata")
        if isinstance(metadata_value, str):
            try:
                feature["metadata"] = json.loads(metadata_value)
            except json.JSONDecodeError:
                # Keep original value so validator reports the failure.
                pass

        # Geometry only needs to be non-null for this schema set.
        geom = feature.get("geometry")
        if geom is not None:
            feature["geometry"] = geom.__geo_interface__
        else:
            feature["geometry"] = None

        try:
            validate(feature)

        except JsonSchemaException as e:
            error_count += 1

            path_key = (layer,) + tuple(e.path)
            path_fail_counts[path_key] = path_fail_counts.get(path_key, 0) + 1

            # After N rows for the same failing element, suppress further row-level logs.
            if path_fail_counts[path_key] > max_rows_per_path:
                if path_key not in suppressed_paths:
                    suppressed_message = (
                        f"\nSuppressed further output for layer '{layer}' path {list(e.path)} "
                        f"after {max_rows_per_path} rows."
                    )
                    print(suppressed_message)
                    write_error_log([suppressed_message])
                    suppressed_paths.add(path_key)
                continue

            error_lines = [
                f"",
                f"Layer   : {layer}",
                f"Row     : {idx}",
                f"Path    : {e.path}",
                f"Message : {e.message}",
                f"Value   : {e.value}",
            ]
            for line in error_lines:
                print(line)
            write_error_log(error_lines)

            # Emit precise metadata failure details when top-level anyOf hides it.
            if list(e.path) == ["data", "metadata"]:
                metadata = feature.get("metadata")
                if isinstance(metadata, str):
                    metadata_message = "Metadata value is a string; expected an array or null."
                    print(metadata_message)
                    write_error_log([metadata_message])
                if isinstance(metadata, list):
                    for i, item in enumerate(metadata):
                        meta_message, meta_value = validate_metadata_item(item)
                        if meta_message is not None:
                            metadata_error_lines = [
                                f"Metadata item {i} failed",
                                "  Path    : ['data', 'metadata', 'items']",
                                f"  Message : {meta_message}",
                                f"  Value   : {meta_value}",
                            ]
                            for line in metadata_error_lines:
                                print(line)
                            write_error_log(metadata_error_lines)

    layer_error_counts[layer] = error_count - layer_errors_before
    layers_validated += 1

print("\nValidation complete.")
print(f"Layers validated: {layers_validated}")
print(f"Layers skipped  : {layers_skipped}")
for layer_name, layer_errors in layer_error_counts.items():
    print(f"  {layer_name}: {layer_errors} error(s)")
print(f"Total errors found: {error_count}")
print(f"Error log written: {error_log_path.resolve()}")