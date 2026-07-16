"""
Generate Pydantic model classes from JSON schemas.

This script reads per-layer JSON schema files and generates explicit Pydantic
model class definitions, writing them to pydantic_models_classes.py.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

# Constants
DEFAULT_EXTERNAL_SCHEMA_DIR = Path("C:/Data/toposource/schema_model")
OUTPUT_FILE = Path(__file__).resolve().parent / "pydantic_models_classes.py"
GEOMETRY_TYPE_NAMES = {"point", "linestring", "polygon", "multilinestring"}


def _resolve_schema_dir() -> Path:
    """Resolve schema directory from env override or default location."""
    configured_dir = os.getenv("TOPOGRAPHIC_SCHEMA_DIR")
    return Path(configured_dir) if configured_dir else DEFAULT_EXTERNAL_SCHEMA_DIR


def _to_class_name(name: str) -> str:
    """Convert schema name to Python class name."""
    parts = re.split(r"[^0-9a-zA-Z]+", name.strip())
    class_name = "".join(part[:1].upper() + part[1:] for part in parts if part)
    if not class_name:
        class_name = "GeneratedModel"
    if class_name[0].isdigit():
        class_name = f"N{class_name}"
    return class_name


def _wrap_optional(type_str: str) -> str:
    if type_str.startswith("Optional["):
        return type_str
    return f"Optional[{type_str}]"


def _union_types(type_names: list[str]) -> str:
    unique_types: list[str] = []
    for type_name in type_names:
        if type_name not in unique_types:
            unique_types.append(type_name)

    if not unique_types:
        return "Any"
    if len(unique_types) == 1:
        return unique_types[0]
    return f"Union[{', '.join(unique_types)}]"


def _get_type_annotation(
    field_schema: dict[str, Any],
    required: bool,
    defs: dict[str, Any],
    model_key: str,
) -> str:
    """Generate type annotation string for a field, including $ref and anyOf."""
    type_str = _schema_to_type_annotation(field_schema, defs, model_key)
    if not required:
        return _wrap_optional(type_str)
    return type_str


def _schema_to_type_annotation(
    schema_fragment: dict[str, Any], defs: dict[str, Any], model_key: str
) -> str:
    ref = schema_fragment.get("$ref")
    if isinstance(ref, str):
        def_name = _schema_ref_to_def_name(ref)
        if def_name and def_name in defs:
            def_schema = defs[def_name]
            if isinstance(def_schema, dict) and def_schema.get("type") == "object" and "properties" in def_schema:
                return _to_class_name(f"{model_key}_{def_name}")
            if isinstance(def_schema, dict):
                return _schema_to_type_annotation(def_schema, defs, model_key)
        return "Any"

    any_of = schema_fragment.get("anyOf")
    if isinstance(any_of, list):
        has_null = False
        variant_types: list[str] = []
        for item in any_of:
            if not isinstance(item, dict):
                continue
            if str(item.get("type", "")).lower() == "null":
                has_null = True
                continue
            variant_types.append(_schema_to_type_annotation(item, defs, model_key))

        base_type = _union_types(variant_types)
        if has_null:
            return _wrap_optional(base_type)
        return base_type

    schema_type = str(schema_fragment.get("type", "")).lower()

    if schema_type == "string":
        return "str"
    if schema_type == "integer":
        return "int"
    if schema_type == "number":
        return "float"
    if schema_type == "boolean":
        return "bool"
    if schema_type == "array":
        items = schema_fragment.get("items")
        if isinstance(items, dict):
            item_type = _schema_to_type_annotation(items, defs, model_key)
        else:
            item_type = "Any"
        return f"list[{item_type}]"
    if schema_type in GEOMETRY_TYPE_NAMES or schema_type == "object":
        return "dict[str, Any]"

    return "Any"


def _generate_field_line(
    field_name: str,
    field_schema: dict[str, Any],
    required: bool,
    defs: dict[str, Any],
    model_key: str,
) -> str:
    """Generate a single Field definition line."""
    type_annotation = _get_type_annotation(field_schema, required, defs, model_key)
    description = field_schema.get("description", "")
    max_length = field_schema.get("maxLength")
    
    # Build Field arguments
    field_args = []
    
    if required:
        field_args.append("...")
    else:
        field_args.append("None")
    
    if description:
        # Use JSON encoding to safely escape quotes/newlines for Python source output.
        desc_literal = json.dumps(str(description), ensure_ascii=False)
        field_args.append(f"description={desc_literal}")
    
    if max_length is not None:
        field_args.append(f"max_length={max_length}")
    
    field_def = f"Field({', '.join(field_args)})"
    return f"    {field_name}: {type_annotation} = {field_def}"


def _generate_model_class(
    class_name: str,
    schema: dict[str, Any],
    defs: dict[str, Any],
    model_key: str,
) -> str:
    """Generate a complete model class definition."""
    lines = []
    
    # Class definition
    lines.append(f"class {class_name}(BaseTopoModel):")
    
    # Docstring
    doc = schema.get("description", f"Generated model for {class_name}.")
    doc_literal = json.dumps(str(doc), ensure_ascii=False)
    lines.append(f"    __doc__ = {doc_literal}")
    lines.append("")
    
    # Fields
    required_fields = set(schema.get("required", []))
    properties = schema.get("properties", {})
    
    if not properties:
        lines.append("    pass")
    else:
        for field_name, field_schema in properties.items():
            is_required = field_name in required_fields
            field_line = _generate_field_line(field_name, field_schema, is_required, defs, model_key)
            lines.append(field_line)
    
    return "\n".join(lines)


def _schema_ref_to_def_name(ref: str) -> str | None:
    prefix = "#/$defs/"
    if not ref.startswith(prefix):
        return None
    return ref[len(prefix) :]


def _extract_model_schemas(
    schema: dict[str, Any], schema_file: Path
) -> list[tuple[str, dict[str, Any]]]:
    """Extract model schemas from object-only and combined JSON schemas."""

    if schema.get("type") == "object" and "properties" in schema:
        model_key = str(schema.get("title") or schema_file.stem.replace("_schema", ""))
        return [(model_key, schema)]

    defs = schema.get("$defs", {})
    if not isinstance(defs, dict):
        return []

    feature_names: list[str] = []
    any_of = schema.get("anyOf")
    if isinstance(any_of, list):
        for item in any_of:
            if not isinstance(item, dict):
                continue
            ref = item.get("$ref")
            if not isinstance(ref, str):
                continue
            def_name = _schema_ref_to_def_name(ref)
            if def_name and def_name in defs:
                feature_names.append(def_name)

    if not feature_names:
        feature_names = sorted(name for name, value in defs.items() if isinstance(value, dict))

    models = []
    for def_name in feature_names:
        def_schema = defs.get(def_name)
        if not isinstance(def_schema, dict):
            continue
        if def_schema.get("type") != "object" or "properties" not in def_schema:
            continue
        models.append((def_name, def_schema))

    return models


def _collect_referenced_def_names(
    schema_fragment: dict[str, Any], defs: dict[str, Any], seen: set[str] | None = None
) -> set[str]:
    """Collect recursively referenced $defs names from a schema fragment."""
    if seen is None:
        seen = set()

    for key, value in schema_fragment.items():
        if key == "$ref" and isinstance(value, str):
            def_name = _schema_ref_to_def_name(value)
            if def_name and def_name in defs and def_name not in seen:
                seen.add(def_name)
                def_schema = defs.get(def_name)
                if isinstance(def_schema, dict):
                    _collect_referenced_def_names(def_schema, defs, seen)
            continue

        if isinstance(value, dict):
            _collect_referenced_def_names(value, defs, seen)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    _collect_referenced_def_names(item, defs, seen)

    return seen


def _iter_schema_entries(schema_dir: Path) -> list[tuple[str, dict[str, Any], dict[str, Any], Path]]:
    schema_files = sorted(schema_dir.glob("*_schema.json"))
    if not schema_files:
        schema_files = sorted(schema_dir.glob("*.json"))
    if not schema_files:
        schema_files = sorted(schema_dir.rglob("*.json"))

    entries: list[tuple[str, dict[str, Any], dict[str, Any], Path]] = []
    for schema_file in schema_files:
        with schema_file.open("r", encoding="utf-8-sig") as f:
            schema = json.load(f)

        defs = schema.get("$defs", {})
        if not isinstance(defs, dict):
            defs = {}

        for schema_key, schema_fragment in _extract_model_schemas(schema, schema_file):
            entries.append((schema_key, schema_fragment, defs, schema_file))

    return entries


def generate_models_file(schema_dir: Path, output_file: Path) -> None:
    """Generate pydantic_models_classes.py from JSON schema directory."""
    models = _iter_schema_entries(schema_dir)

    if not models:
        raise RuntimeError(f"No usable schemas found under {schema_dir}")

    # Generate file content
    lines = [
        '"""',
        "Explicit Pydantic model class definitions for topographic features.",
        "",
        "Generated from JSON schemas with proper Field constraints and type hints.",
        '"""',
        "",
        "from __future__ import annotations",
        "",
        "from typing import Any, Optional, Union",
        "",
        "from pydantic import BaseModel, ConfigDict, Field",
        "",
        "",
        "class BaseTopoModel(BaseModel):",
        '    """Base class for all topographic feature models."""',
        "",
        "    model_config = ConfigDict(extra=\"forbid\")",
        "",
        "",
    ]
    
    # Generate model classes
    generated_class_names: set[str] = set()

    for model_key, model_schema, defs, _ in models:
        for def_name in sorted(_collect_referenced_def_names(model_schema, defs)):
            def_schema = defs.get(def_name)
            if not isinstance(def_schema, dict):
                continue
            if def_schema.get("type") != "object" or "properties" not in def_schema:
                continue

            ref_class_name = _to_class_name(f"{model_key}_{def_name}")
            if ref_class_name in generated_class_names:
                continue

            ref_model_code = _generate_model_class(ref_class_name, def_schema, defs, model_key)
            lines.append("")
            lines.append(ref_model_code)
            lines.append("")
            generated_class_names.add(ref_class_name)

        class_name = _to_class_name(model_key)
        if class_name in generated_class_names:
            continue

        model_code = _generate_model_class(class_name, model_schema, defs, model_key)
        lines.append("")
        lines.append(model_code)
        lines.append("")
        generated_class_names.add(class_name)
    
    # Write output file
    content = "\n".join(lines)
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"✓ Generated {len(models)} model classes")
    print(f"✓ Output written to {output_file}")
    print(f"✓ Schema source directory: {schema_dir}")
    print(f"\nGenerated models:")
    for model_key, _, _, schema_file in models:
        class_name = _to_class_name(model_key)
        print(f"  - {class_name} ({schema_file.name})")


if __name__ == "__main__":
    try:
        schema_directory = _resolve_schema_dir()
        generate_models_file(schema_directory, OUTPUT_FILE)
        print("\n✓ Generation complete!")
    except Exception as e:
        print(f"✗ Error: {e}")
        raise
