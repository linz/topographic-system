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


def load_models(schema_dir: Path = SCHEMA_DIR) -> dict[str, type[BaseTopoModel]]:
    """Load all JSON schemas in a folder and generate Pydantic models."""

    models_by_title: dict[str, type[BaseTopoModel]] = {}

    for schema_file in sorted(schema_dir.glob("*_schema.json")):
        with schema_file.open("r", encoding="utf-8") as f:
            schema = json.load(f)

        model = _build_model(schema, schema_file)
        title = str(schema.get("title") or schema_file.stem.replace("_schema", ""))

        models_by_title[title] = model

        # Export each generated class at module level.
        globals()[model.__name__] = model

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
