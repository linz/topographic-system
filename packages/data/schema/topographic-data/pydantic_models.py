from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Annotated, Any, Optional

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, create_model

SCHEMA_DIR = Path(__file__).resolve().parent / "master_json_schema"
GEOMETRY_TYPE_NAMES = {"point", "linestring", "polygon", "multilinestring"}


class BaseTopoModel(BaseModel):
    """Base class for all generated topographic feature models."""

    model_config = ConfigDict(extra="forbid")


def _to_class_name(name: str) -> str:
    parts = re.split(r"[^0-9a-zA-Z]+", name.strip())
    class_name = "".join(part[:1].upper() + part[1:] for part in parts if part)
    if not class_name:
        class_name = "GeneratedModel"
    if class_name[0].isdigit():
        class_name = f"N{class_name}"
    return class_name


def _string_type(max_length: int | None) -> Any:
    if max_length is None:
        return str
    return Annotated[str, StringConstraints(max_length=max_length)]


def _resolve_field_type(field_schema: dict[str, Any]) -> Any:
    schema_type = str(field_schema.get("type", "")).lower()

    if schema_type == "string":
        return _string_type(field_schema.get("maxLength"))
    if schema_type == "integer":
        return int
    if schema_type == "number":
        return float
    if schema_type in GEOMETRY_TYPE_NAMES:
        # Geometry fields use custom schema type names in source JSON schemas.
        return dict[str, Any]

    raise ValueError(f"Unsupported schema type '{schema_type}'")


def _build_model(schema: dict[str, Any], schema_file: Path) -> type[BaseTopoModel]:
    if schema.get("type") != "object":
        raise ValueError(f"Only object schemas are supported: {schema_file.name}")

    required_fields = set(schema.get("required", []))
    properties: dict[str, dict[str, Any]] = schema.get("properties", {})
    fields: dict[str, tuple[Any, Any]] = {}

    for field_name, field_schema in properties.items():
        field_type = _resolve_field_type(field_schema)
        description = field_schema.get("description")
        schema_type = str(field_schema.get("type", "")).lower()

        extra: dict[str, Any] = {}
        if "precision" in field_schema:
            extra["precision"] = field_schema["precision"]
        if schema_type in GEOMETRY_TYPE_NAMES:
            extra["geometry_type"] = schema_type

        json_schema_extra = extra if extra else None

        if field_name in required_fields:
            fields[field_name] = (
                field_type,
                Field(..., description=description, json_schema_extra=json_schema_extra),
            )
        else:
            fields[field_name] = (
                Optional[field_type],
                Field(None, description=description, json_schema_extra=json_schema_extra),
            )

    title = schema.get("title") or schema_file.stem.replace("_schema", "")
    class_name = _to_class_name(title)

    model = create_model(class_name, __base__=BaseTopoModel, **fields)
    model.__doc__ = schema.get("description") or f"Generated model for {title}."
    return model


def _schema_ref_to_def_name(ref: str) -> str | None:
    prefix = "#/$defs/"
    if not ref.startswith(prefix):
        return None
    return ref[len(prefix) :]


def _extract_feature_schemas(
    schema: dict[str, Any], schema_file: Path
) -> list[tuple[str, dict[str, Any]]]:
    """Extract (schema_key, object-schema) pairs from legacy or combined schema JSON."""

    if schema.get("type") == "object" and "properties" in schema:
        return [(schema_file.stem.replace("_schema", ""), schema)]

    defs = schema.get("$defs")
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

    extracted: list[tuple[str, dict[str, Any]]] = []
    for def_name in feature_names:
        candidate = defs.get(def_name)
        if not isinstance(candidate, dict):
            continue
        if candidate.get("type") != "object" or "properties" not in candidate:
            continue
        extracted.append((def_name, candidate))

    return extracted


def _iter_schema_entries(schema_dir: Path) -> list[tuple[str, dict[str, Any], Path]]:
    schema_files = sorted(schema_dir.glob("*_schema.json"))
    if not schema_files:
        schema_files = sorted(schema_dir.glob("*.json"))

    entries: list[tuple[str, dict[str, Any], Path]] = []
    for schema_file in schema_files:
        with schema_file.open("r", encoding="utf-8") as f:
            schema = json.load(f)

        for schema_key, schema_fragment in _extract_feature_schemas(schema, schema_file):
            entries.append((schema_key, schema_fragment, schema_file))

    return entries


def load_models(schema_dir: Path = SCHEMA_DIR) -> dict[str, type[BaseTopoModel]]:
    """Load JSON schemas and generate Pydantic models.

    Supports both legacy one-schema-per-file format and combined schema files
    that define feature schemas under ``$defs``.
    """

    models_by_title: dict[str, type[BaseTopoModel]] = {}

    for schema_key, schema, schema_file in _iter_schema_entries(schema_dir):
        model = _build_model(schema, schema_file)
        title = str(schema.get("title") or schema_key)

        if title in models_by_title:
            raise ValueError(f"Duplicate model title '{title}' in {schema_file.name}")

        models_by_title[title] = model

        # Export each generated class at module level.
        globals()[model.__name__] = model

    if not models_by_title:
        raise RuntimeError(f"No usable schemas found under {schema_dir}")

    return models_by_title


MODELS_BY_TITLE = load_models()
MODELS_BY_CLASS_NAME = {model.__name__: model for model in MODELS_BY_TITLE.values()}
MODELS_BY_SCHEMA_FILE = {
    f"{name}_schema": model for name, model in MODELS_BY_TITLE.items()
}


def get_model(name: str) -> type[BaseTopoModel]:
    """Return a generated model by title, class name, or '<title>_schema'."""

    if name in MODELS_BY_TITLE:
        return MODELS_BY_TITLE[name]
    if name in MODELS_BY_CLASS_NAME:
        return MODELS_BY_CLASS_NAME[name]
    if name in MODELS_BY_SCHEMA_FILE:
        return MODELS_BY_SCHEMA_FILE[name]

    raise KeyError(f"No model found for '{name}'")


__all__ = [
    "BaseTopoModel",
    "MODELS_BY_TITLE",
    "MODELS_BY_CLASS_NAME",
    "MODELS_BY_SCHEMA_FILE",
    "load_models",
    "get_model",
    *sorted(model.__name__ for model in MODELS_BY_TITLE.values()),
]
