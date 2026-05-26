"""
Generate Pydantic model classes from JSON schemas.

This script reads the master JSON schema and generates explicit Pydantic model
class definitions, writing them to pydantic_models_classes.py.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

# Constants
SCHEMA_FILE = Path(__file__).resolve().parent / "master_json_schema" / "topographic_data_schema.json"
OUTPUT_FILE = Path(__file__).resolve().parent / "pydantic_models_classes.py"
GEOMETRY_TYPE_NAMES = {"point", "linestring", "polygon", "multilinestring"}


def _to_class_name(name: str) -> str:
    """Convert schema name to Python class name."""
    parts = re.split(r"[^0-9a-zA-Z]+", name.strip())
    class_name = "".join(part[:1].upper() + part[1:] for part in parts if part)
    if not class_name:
        class_name = "GeneratedModel"
    if class_name[0].isdigit():
        class_name = f"N{class_name}"
    return class_name


def _get_type_annotation(field_schema: dict[str, Any], required: bool) -> str:
    """Generate type annotation string for a field."""
    schema_type = str(field_schema.get("type", "")).lower()
    
    if schema_type == "string":
        type_str = "str"
    elif schema_type == "integer":
        type_str = "int"
    elif schema_type == "number":
        type_str = "float"
    elif schema_type in GEOMETRY_TYPE_NAMES:
        type_str = "dict[str, Any]"
    else:
        type_str = "Any"
    
    if not required:
        return f"Optional[{type_str}]"
    return type_str


def _generate_field_line(field_name: str, field_schema: dict[str, Any], required: bool) -> str:
    """Generate a single Field definition line."""
    type_annotation = _get_type_annotation(field_schema, required)
    description = field_schema.get("description", "")
    max_length = field_schema.get("maxLength")
    
    # Build Field arguments
    field_args = []
    
    if required:
        field_args.append("...")
    else:
        field_args.append("None")
    
    if description:
        # Escape quotes in description
        desc_escaped = description.replace('"', '\\"')
        field_args.append(f'description="{desc_escaped}"')
    
    if max_length is not None:
        field_args.append(f"max_length={max_length}")
    
    field_def = f"Field({', '.join(field_args)})"
    return f"    {field_name}: {type_annotation} = {field_def}"


def _generate_model_class(class_name: str, schema: dict[str, Any]) -> str:
    """Generate a complete model class definition."""
    lines = []
    
    # Class definition
    lines.append(f"class {class_name}(BaseTopoModel):")
    
    # Docstring
    doc = schema.get("description", f"Generated model for {class_name}.")
    lines.append(f'    """{doc}"""')
    lines.append("")
    
    # Fields
    required_fields = set(schema.get("required", []))
    properties = schema.get("properties", {})
    
    if not properties:
        lines.append("    pass")
    else:
        for field_name in sorted(properties.keys()):
            field_schema = properties[field_name]
            is_required = field_name in required_fields
            field_line = _generate_field_line(field_name, field_schema, is_required)
            lines.append(field_line)
    
    return "\n".join(lines)


def _extract_model_schemas(schema: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """Extract model schemas from the root schema."""
    defs = schema.get("$defs", {})
    if not isinstance(defs, dict):
        return []
    
    models = []
    for def_name, def_schema in sorted(defs.items()):
        if not isinstance(def_schema, dict):
            continue
        if def_schema.get("type") != "object" or "properties" not in def_schema:
            continue
        models.append((def_name, def_schema))
    
    return models


def generate_models_file(schema_file: Path, output_file: Path) -> None:
    """Generate pydantic_models_classes.py from JSON schema."""
    
    # Read schema
    with open(schema_file, "r", encoding="utf-8-sig") as f:
        schema = json.load(f)
    
    # Extract models
    models = _extract_model_schemas(schema)
    
    if not models:
        raise RuntimeError(f"No usable schemas found in {schema_file}")
    
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
        "from typing import Any, Optional",
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
    for def_name, model_schema in models:
        class_name = _to_class_name(def_name)
        model_code = _generate_model_class(class_name, model_schema)
        lines.append("")
        lines.append(model_code)
        lines.append("")
    
    # Write output file
    content = "\n".join(lines)
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(content)
    
    print(f"✓ Generated {len(models)} model classes")
    print(f"✓ Output written to {output_file}")
    print(f"\nGenerated models:")
    for def_name, _ in models:
        class_name = _to_class_name(def_name)
        print(f"  - {class_name}")


if __name__ == "__main__":
    try:
        generate_models_file(SCHEMA_FILE, OUTPUT_FILE)
        print("\n✓ Generation complete!")
    except Exception as e:
        print(f"✗ Error: {e}")
        raise
