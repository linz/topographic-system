import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
import pandas as pd
import pyogrio
import fastjsonschema
from fastjsonschema import JsonSchemaException


class GpkgValidator:

    def __init__(
        self,
        gpkg_path: str | Path = r"C:\Data\toposource\topographic-data",
        gpkg_db: str = "topographic-data.gpkg",
        schema_dir: str | Path = r"C:\Data\toposource\schema_model",
        error_log_path: str | Path = "val_errors.log",
    ):
        db_file = Path(gpkg_db)
        if db_file.suffix.lower() != ".gpkg":
            db_file = db_file.with_suffix(".gpkg")
        self.gpkg_path = Path(gpkg_path) / db_file
        self.schema_dir = Path(schema_dir)
        self.error_log_path = Path(error_log_path)

        self.error_log_path.write_text("", encoding="utf-8")
        self.error_count = 0
        self.max_rows_per_path = 10

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def write_error_log(self, lines):
        with self.error_log_path.open("a", encoding="utf-8") as log_file:
            for line in lines:
                log_file.write(f"{line}\n")

    @staticmethod
    def normalize_name(name):
        normalized = str(name).replace("\\", "/").strip()
        if normalized.lower().startswith("next/"):
            normalized = normalized[5:]
        return normalized

    @staticmethod
    def normalize_next_uri(value):
        if not isinstance(value, str):
            return value

        # Collapse repeated next/ prefixes and convert to a stable absolute URI.
        normalized = value.replace("\\", "/").strip()
        while normalized.lower().startswith("next/"):
            normalized = normalized[5:]
        return f"https://schemas.next.local/{normalized}"

    def sanitize_schema_uris(self, value):
        if isinstance(value, dict):
            sanitized = {}
            for key, item in value.items():
                if key == "$id":
                    sanitized[key] = self.normalize_next_uri(item)
                elif key == "$ref" and isinstance(item, str) and item.lower().startswith("next/"):
                    sanitized[key] = self.normalize_next_uri(item)
                else:
                    sanitized[key] = self.sanitize_schema_uris(item)
            return sanitized

        if isinstance(value, list):
            return [self.sanitize_schema_uris(item) for item in value]

        return value

    @staticmethod
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
                return GpkgValidator.normalize_json_value(value.item())
            except (ValueError, TypeError):
                pass

        if isinstance(value, dict):
            return {k: GpkgValidator.normalize_json_value(v) for k, v in value.items()}

        if isinstance(value, list):
            return [GpkgValidator.normalize_json_value(v) for v in value]

        return value

    @staticmethod
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

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_feature(
        self,
        layer: str,
        idx,
        feature: dict,
        validate,
        path_fail_counts: dict,
        suppressed_paths: set,
    ) -> tuple[int, list[str], list[str]]:
        """Returns (error_count, output_lines, log_lines) without touching shared state."""
        output_lines: list[str] = []
        log_lines: list[str] = []

        try:
            validate(feature)
        except JsonSchemaException as e:
            path_key = (layer,) + tuple(e.path)
            path_fail_counts[path_key] = path_fail_counts.get(path_key, 0) + 1

            # After N rows for the same failing element, suppress further row-level logs.
            if path_fail_counts[path_key] > self.max_rows_per_path:
                if path_key not in suppressed_paths:
                    suppressed_message = (
                        f"\nSuppressed further output for layer '{layer}' path {list(e.path)} "
                        f"after {self.max_rows_per_path} rows."
                    )
                    output_lines.append(suppressed_message)
                    log_lines.append(suppressed_message)
                    suppressed_paths.add(path_key)
                return 1, output_lines, log_lines

            error_lines = [
                "",
                f"Layer   : {layer}",
                f"Row     : {idx}",
                f"Path    : {e.path}",
                f"Message : {e.message}",
                f"Value   : {e.value}",
            ]
            output_lines.extend(error_lines)
            log_lines.extend(error_lines)

            # Emit precise metadata failure details when top-level anyOf hides it.
            if list(e.path) == ["data", "metadata"]:
                metadata = feature.get("metadata")
                if isinstance(metadata, str):
                    metadata_message = "Metadata value is a string; expected an array or null."
                    output_lines.append(metadata_message)
                    log_lines.append(metadata_message)
                if isinstance(metadata, list):
                    for i, item in enumerate(metadata):
                        meta_message, meta_value = self.validate_metadata_item(item)
                        if meta_message is not None:
                            metadata_error_lines = [
                                f"Metadata item {i} failed",
                                "  Path    : ['data', 'metadata', 'items']",
                                f"  Message : {meta_message}",
                                f"  Value   : {meta_value}",
                            ]
                            output_lines.extend(metadata_error_lines)
                            log_lines.extend(metadata_error_lines)

            return 1, output_lines, log_lines

        return 0, output_lines, log_lines

    def _validate_layer(
        self, layer: str, schema_path: Path
    ) -> tuple[int, list[str], list[str]]:
        """Returns (error_count, output_lines, log_lines) — fully self-contained."""
        with schema_path.open("r", encoding="utf-8") as f:
            schema = self.sanitize_schema_uris(json.load(f))
        validate = fastjsonschema.compile(schema)

        gdf = pyogrio.read_dataframe(self.gpkg_path, layer=layer)

        # Ensure datetime columns are JSON/schema friendly before validation.
        for col in ["created_at", "updated_at"]:
            if col in gdf.columns:
                col_utc = pd.to_datetime(gdf[col], errors="coerce", utc=True)
                gdf[col] = col_utc.apply(
                    lambda value: value.isoformat().replace("+00:00", "Z") if pd.notna(value) else None
                )

        output_lines = [f"\nValidating layer '{layer}' ({len(gdf)} feature(s))"]
        log_lines: list[str] = []
        error_count = 0
        path_fail_counts: dict = {}
        suppressed_paths: set = set()

        for idx, row in gdf.iterrows():
            # Convert row to dictionary, normalizing pandas missing values to JSON null.
            feature = self.normalize_json_value(row.where(pd.notna(row), None).to_dict())

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
            feature["geometry"] = geom.__geo_interface__ if geom is not None else None

            errors, out, log = self._validate_feature(
                layer, idx, feature, validate, path_fail_counts, suppressed_paths
            )
            error_count += errors
            output_lines.extend(out)
            log_lines.extend(log)

        return error_count, output_lines, log_lines

    def run(self):
        if not self.gpkg_path.exists():
            raise FileNotFoundError(f"GeoPackage not found: {self.gpkg_path}")
        if not self.schema_dir.exists():
            raise FileNotFoundError(f"Schema directory not found: {self.schema_dir}")

        layer_info = pyogrio.list_layers(self.gpkg_path)
        layers = [item[0] if not isinstance(item, str) else item for item in layer_info]

        schema_map = {}
        for schema_file in self.schema_dir.rglob("*.json"):
            rel_no_ext = schema_file.relative_to(self.schema_dir).with_suffix("")
            schema_key = self.normalize_name(rel_no_ext)
            if schema_key not in schema_map:
                schema_map[schema_key] = schema_file

        print(f"Found {len(layers)} layer(s) in {self.gpkg_path.name}")

        layers_validated = 0
        layers_skipped = 0
        layer_error_counts = {}

        work: dict = {}
        for layer in layers:
            layer_key = self.normalize_name(layer)
            schema_path = schema_map.get(layer_key)
            if schema_path is None:
                layers_skipped += 1
                print(f"\nSkipping layer '{layer}': schema '{layer_key}.json' not found")
            else:
                work[layer] = schema_path

        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(self._validate_layer, layer, schema_path): layer
                for layer, schema_path in work.items()
            }
            for future in as_completed(futures):
                layer = futures[future]
                error_count, output_lines, log_lines = future.result()
                for line in output_lines:
                    print(line)
                self.write_error_log(log_lines)
                self.error_count += error_count
                layer_error_counts[layer] = error_count
                layers_validated += 1

        print("\nValidation complete.")
        print(f"Layers validated: {layers_validated}")
        print(f"Layers skipped  : {layers_skipped}")
        for layer_name, layer_errors in layer_error_counts.items():
            print(f"  {layer_name}: {layer_errors} error(s)")
        print(f"Total errors found: {self.error_count}")
        print(f"Error log written: {self.error_log_path.resolve()}")


if __name__ == "__main__":
    GpkgValidator(
        gpkg_path=r"C:\Data\toposource\topographic-data",
        gpkg_db="topographic-data.gpkg",
        schema_dir=r"C:\Data\toposource\schema_model",
        error_log_path="val_errors.log",
    ).run()