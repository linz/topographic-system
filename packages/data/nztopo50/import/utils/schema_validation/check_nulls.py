import argparse
import csv
import json
import sqlite3
from pathlib import Path


DEFAULT_SCHEMA_DIRECTORY = Path(r"C:\Data\repos\topographic-system\schema\v0.4")
DEFAULT_GPKG_PATH = Path(
    r"C:\Data\toposource\topographic-data-dev\topographic-data-dev.gpkg"
)
DEFAULT_OUTPUT_DIRECTORY = Path(r"C:\temp\export")


def allows_null(field_schema: object) -> bool:
    if not isinstance(field_schema, dict):
        return False

    field_type = field_schema.get("type")
    if field_type == "null":
        return True
    if isinstance(field_type, list) and "null" in field_type:
        return True

    return any(
        allows_null(option)
        for keyword in ("anyOf", "oneOf")
        for option in field_schema.get(keyword, [])
    )


def resolve_local_reference(schema: dict[str, object], reference: str) -> object:
    if not reference.startswith("#/"):
        raise ValueError(f"Only local schema references are supported: {reference}")

    value: object = schema
    for part in reference[2:].split("/"):
        if not isinstance(value, dict) or part not in value:
            raise ValueError(f"Schema reference not found: {reference}")
        value = value[part]
    return value


def collect_property_schemas(
    schema: dict[str, object], node: object
) -> dict[str, list[object]]:
    if not isinstance(node, dict):
        return {}

    reference = node.get("$ref")
    if isinstance(reference, str):
        return collect_property_schemas(
            schema, resolve_local_reference(schema, reference)
        )

    properties = node.get("properties")
    if isinstance(properties, dict):
        return {name: [field_schema] for name, field_schema in properties.items()}

    collected: dict[str, list[object]] = {}
    for keyword in ("allOf", "anyOf", "oneOf"):
        options = node.get(keyword, [])
        if not isinstance(options, list):
            continue
        for option in options:
            for field_name, field_schemas in collect_property_schemas(
                schema, option
            ).items():
                collected.setdefault(field_name, []).extend(field_schemas)
    return collected


def find_nullable_fields(schema_directory: Path) -> dict[str, list[str]]:
    if not schema_directory.is_dir():
        raise FileNotFoundError(f"Schema directory not found: {schema_directory}")

    schema_files = sorted(schema_directory.glob("*.json"))
    if not schema_files:
        raise ValueError(f"No JSON schemas found in {schema_directory}")

    nullable_fields = {}
    for schema_file in schema_files:
        with schema_file.open(encoding="utf-8") as file:
            schema = json.load(file)

        property_schemas = collect_property_schemas(schema, schema)
        if not property_schemas:
            raise ValueError(f"Schema has no properties object: {schema_file}")

        nullable_fields[schema_file.stem] = [
            field_name
            for field_name, field_schemas in property_schemas.items()
            if any(allows_null(field_schema) for field_schema in field_schemas)
        ]

    return nullable_fields


def quote_identifier(identifier: str) -> str:
    escaped_identifier = identifier.replace('"', '""')
    return f'"{escaped_identifier}"'


def get_null_results(
    gpkg_path: Path, nullable_fields: dict[str, list[str]]
) -> list[dict[str, object]]:
    if not gpkg_path.is_file():
        raise FileNotFoundError(f"GeoPackage not found: {gpkg_path}")

    results = []
    database_uri = f"{gpkg_path.resolve().as_uri()}?mode=ro"
    with sqlite3.connect(database_uri, uri=True) as connection:
        layers = {
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM gpkg_contents"
            ).fetchall()
        }

        for layer, fields in nullable_fields.items():
            if layer not in layers:
                results.extend(
                    {
                        "layer": layer,
                        "field": field,
                        "null_count": "",
                        "result": "MissingLayer",
                    }
                    for field in fields
                )
                continue

            columns = {
                row[1]
                for row in connection.execute(
                    f"PRAGMA table_info({quote_identifier(layer)})"
                ).fetchall()
            }
            missing_fields = [field for field in fields if field not in columns]
            if missing_fields:
                missing_list = ", ".join(missing_fields)
                raise ValueError(
                    f"Layer {layer!r} is missing nullable field(s): {missing_list}"
                )
            if not fields:
                continue

            count_expressions = ", ".join(
                f"COUNT(*) FILTER (WHERE {quote_identifier(field)} IS NULL)"
                for field in fields
            )
            query = f"SELECT {count_expressions} FROM {quote_identifier(layer)}"
            null_counts = connection.execute(query).fetchone()
            if null_counts is None:
                raise RuntimeError(f"Null query returned no result for layer {layer!r}")

            results.extend(
                {
                    "layer": layer,
                    "field": field,
                    "null_count": null_count,
                    "result": "hasNulls" if null_count else "NoNulls",
                }
                for field, null_count in zip(fields, null_counts, strict=True)
            )

    return results


def write_nullable_fields(
    output_path: Path, nullable_fields: dict[str, list[str]]
) -> None:
    with output_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=["layer", "field"])
        writer.writeheader()
        writer.writerows(
            {"layer": layer, "field": field}
            for layer, fields in nullable_fields.items()
            for field in fields
        )


def write_results(output_path: Path, results: list[dict[str, object]]) -> None:
    with output_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file, fieldnames=["layer", "field", "null_count", "result"]
        )
        writer.writeheader()
        writer.writerows(results)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find nullable schema fields and check them for NULL values."
    )
    parser.add_argument(
        "--schema-directory",
        type=Path,
        default=DEFAULT_SCHEMA_DIRECTORY,
        help="Directory containing layer JSON schemas (default: %(default)s)",
    )
    parser.add_argument(
        "--gpkg",
        type=Path,
        default=DEFAULT_GPKG_PATH,
        help="GeoPackage to inspect (default: %(default)s)",
    )
    parser.add_argument(
        "--output-directory",
        type=Path,
        default=DEFAULT_OUTPUT_DIRECTORY,
        help="Directory for CSV reports (default: %(default)s)",
    )
    return parser.parse_args()


def main() -> None:
    arguments = parse_args()
    arguments.output_directory.mkdir(parents=True, exist_ok=True)

    nullable_fields = find_nullable_fields(arguments.schema_directory)
    nullable_fields_path = arguments.output_directory / "nullable_fields.csv"
    results_path = arguments.output_directory / "nullable_field_null_results.csv"

    write_nullable_fields(nullable_fields_path, nullable_fields)
    results = get_null_results(arguments.gpkg, nullable_fields)
    write_results(results_path, results)

    print(
        f"Wrote {sum(len(fields) for fields in nullable_fields.values())} nullable "
        f"fields to {nullable_fields_path}"
    )
    print(f"Wrote {len(results)} NULL check results to {results_path}")


if __name__ == "__main__":
    main()