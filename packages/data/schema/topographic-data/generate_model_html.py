from __future__ import annotations

import argparse
import html
import importlib.util
import sys
from pathlib import Path
from typing import Any

from pydantic import BaseModel


def _load_pydantic_models_module(models_file: Path):
    # Read file with UTF-8 BOM handling.
    content = models_file.read_text(encoding="utf-8-sig")

    spec = importlib.util.spec_from_file_location("pydantic_models", models_file)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {models_file}")

    module = importlib.util.module_from_spec(spec)
    # Register module so Pydantic can resolve forward refs via __module__ lookups.
    sys.modules[spec.name] = module

    # Execute module with content that has BOM removed.
    exec(compile(content, str(models_file), "exec"), module.__dict__)
    return module


def _rebuild_models(models_by_title: dict[str, type[BaseModel]], module: Any) -> None:
    """Rebuild models to resolve postponed annotations/forward refs."""
    types_namespace = dict(module.__dict__)
    for model_class in models_by_title.values():
        model_class.model_rebuild(force=True, _types_namespace=types_namespace)


def _json_type_to_text(property_schema: dict[str, Any]) -> str:
    return _schema_fragment_type_to_text(property_schema)


def _schema_ref_to_name(ref: str) -> str:
    return ref.rsplit("/", 1)[-1] if "/" in ref else ref


def _schema_fragment_type_to_text(schema_fragment: dict[str, Any]) -> str:
    ref = schema_fragment.get("$ref")
    if isinstance(ref, str):
        return _schema_ref_to_name(ref)

    any_of = schema_fragment.get("anyOf")
    if isinstance(any_of, list):
        has_null = False
        variants: list[str] = []
        for item in any_of:
            if not isinstance(item, dict):
                continue
            if str(item.get("type", "")).lower() == "null":
                has_null = True
                continue
            type_text = _schema_fragment_type_to_text(item)
            if type_text not in variants:
                variants.append(type_text)

        if not variants:
            return "Optional[Any]" if has_null else "unknown"

        combined = variants[0] if len(variants) == 1 else " | ".join(variants)
        return f"Optional[{combined}]" if has_null else combined

    schema_type = str(schema_fragment.get("type", "")).lower()
    if schema_type == "array":
        items = schema_fragment.get("items")
        if isinstance(items, dict):
            return f"list[{_schema_fragment_type_to_text(items)}]"
        return "list[Any]"

    if schema_type == "object":
        return "object"
    if schema_type == "string" and schema_fragment.get("format") == "date-time":
        return "datetime"
    if schema_type:
        return schema_type

    return "unknown"


def _load_models_by_title(module: Any) -> dict[str, type[BaseModel]]:
    models_by_title = getattr(module, "MODELS_BY_TITLE", None)
    if isinstance(models_by_title, dict):
        return models_by_title

    # Fallback for generated class modules that don't export MODELS_BY_TITLE.
    collected: dict[str, type[BaseModel]] = {}
    for attr_name, attr_value in module.__dict__.items():
        if not isinstance(attr_value, type):
            continue
        if not issubclass(attr_value, BaseModel):
            continue
        if attr_value is BaseModel or attr_name == "BaseTopoModel":
            continue
        collected[attr_name] = attr_value

    if not collected:
        raise RuntimeError("No Pydantic model classes found in the loaded module")

    return collected


def _collect_model_rows(model_class: type[BaseModel]) -> tuple[str, list[dict[str, str]]]:
    doc = (model_class.__doc__ or "").strip()
    model_schema = model_class.model_json_schema()
    required = set(model_schema.get("required", []))
    properties = model_schema.get("properties", {})
    rows: list[dict[str, str]] = []

    for field_name, property_schema in properties.items():
        if not isinstance(property_schema, dict):
            continue

        field_type = _json_type_to_text(property_schema)
        required_text = "yes" if field_name in required else "no"

        if field_name in required:
            default_text = "required"
        elif "default" in property_schema:
            default_text = repr(property_schema.get("default"))
        else:
            default_text = "null"

        description = str(property_schema.get("description", ""))
        max_length = property_schema.get("maxLength")
        max_length_text = str(max_length) if max_length is not None else ""

        extra_parts: list[str] = []
        for key in ("precision", "geometry_type"):
            if key in property_schema:
                value = property_schema[key]
                extra_parts.append(f"{key}={value}")

        rows.append(
            {
                "field": field_name,
                "type": field_type,
                "required": required_text,
                "default": default_text,
                "max_length": max_length_text,
                "description": description,
                "extra": "; ".join(extra_parts),
            }
        )

    return doc, rows


def _render_model_section(model_name: str, model_class: type[BaseModel]) -> str:
    doc, field_rows = _collect_model_rows(model_class)
    rows: list[str] = []

    for field_row in field_rows:
        rows.append(
            "<tr>"
            f"<td>{html.escape(field_row['field'])}</td>"
            f"<td>{html.escape(field_row['type'])}</td>"
            f"<td>{html.escape(field_row['required'])}</td>"
            f"<td>{html.escape(field_row['default'])}</td>"
            f"<td>{html.escape(field_row['max_length'])}</td>"
            f"<td>{html.escape(field_row['description'])}</td>"
            f"<td>{html.escape(field_row['extra'])}</td>"
            "</tr>"
        )

    if not rows:
        rows.append('<tr><td colspan="7">No fields</td></tr>')

    return (
        f"<section id=\"{html.escape(model_name)}\">"
        f"<h2>{html.escape(model_name)}</h2>"
        f"<p>{html.escape(doc)}</p>"
        "<table>"
        "<thead><tr>"
        "<th>Field</th><th>Type</th><th>Required</th><th>Default</th>"
        "<th>Max Length</th><th>Description</th><th>Extra</th>"
        "</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
        "</section>"
    )


def _escape_markdown_table_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ").strip()


def _render_model_markdown_section(model_name: str, model_class: type[BaseModel]) -> str:
    doc, field_rows = _collect_model_rows(model_class)
    lines = [f"## {model_name}", ""]

    if doc:
        lines.append(doc)
        lines.append("")

    lines.extend(
        [
            "| Field | Type | Required | Default | Max Length | Description | Extra |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
    )

    if not field_rows:
        lines.append("| No fields |  |  |  |  |  |  |")
    else:
        for field_row in field_rows:
            lines.append(
                "| "
                + " | ".join(
                    _escape_markdown_table_cell(field_row[key])
                    for key in (
                        "field",
                        "type",
                        "required",
                        "default",
                        "max_length",
                        "description",
                        "extra",
                    )
                )
                + " |"
            )

    lines.append("")
    return "\n".join(lines)


def build_html(models_file: Path, output_file: Path) -> None:
    module = _load_pydantic_models_module(models_file)
    models_by_title = _load_models_by_title(module)
    _rebuild_models(models_by_title, module)

    model_items = sorted(models_by_title.items(), key=lambda item: item[0])
    toc_items = [
        f'<li><a href="#{html.escape(name)}">{html.escape(name)}</a></li>'
        for name, _ in model_items
    ]

    sections = [_render_model_section(name, model_class) for name, model_class in model_items]

    page = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>Topographic Data Models</title>
  <style>
    :root {{
      --bg: #f6f7f9;
      --panel: #ffffff;
      --text: #1f2937;
      --muted: #6b7280;
      --border: #d1d5db;
      --accent: #0f766e;
    }}
    body {{
      margin: 0;
      font-family: "Segoe UI", Tahoma, sans-serif;
      background: var(--bg);
      color: var(--text);
      line-height: 1.5;
    }}
    .wrap {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 24px;
    }}
    header {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 16px;
      margin-bottom: 18px;
    }}
    h1 {{
      margin: 0 0 6px;
      font-size: 28px;
    }}
    .meta {{
      margin: 0;
      color: var(--muted);
      font-size: 14px;
    }}
    nav {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 14px 18px;
      margin-bottom: 18px;
    }}
    nav h2 {{
      margin: 0 0 8px;
      font-size: 20px;
    }}
    nav ul {{
      columns: 3;
      margin: 0;
      padding-left: 20px;
    }}
    nav a {{
      color: var(--accent);
      text-decoration: none;
    }}
    section {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 12px;
      margin-bottom: 14px;
    }}
    section h2 {{
      margin: 4px 0 8px;
      font-size: 22px;
    }}
    section p {{
      margin: 0 0 12px;
      color: var(--muted);
      font-size: 14px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    th, td {{
      border: 1px solid var(--border);
      text-align: left;
      padding: 7px;
      vertical-align: top;
      word-break: break-word;
    }}
    thead th {{
      background: #ecfeff;
      position: sticky;
      top: 0;
    }}
    @media (max-width: 900px) {{
      nav ul {{ columns: 1; }}
    }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <header>
      <h1>Topographic Data Models</h1>
      <p class=\"meta\">Total models: {len(model_items)}</p>
    </header>
    <nav>
      <h2>Models</h2>
      <ul>{''.join(toc_items)}</ul>
    </nav>
    {''.join(sections)}
  </div>
</body>
</html>
"""

    output_file.write_text(page, encoding="utf-8")


def build_markdown(models_file: Path, output_file: Path) -> None:
    module = _load_pydantic_models_module(models_file)
    models_by_title = _load_models_by_title(module)
    _rebuild_models(models_by_title, module)

    model_items = sorted(models_by_title.items(), key=lambda item: item[0])
    toc_items = [f"- [{name}](#{name.lower()})" for name, _ in model_items]
    sections = [
        _render_model_markdown_section(name, model_class)
        for name, model_class in model_items
    ]

    page = "\n".join(
        [
            "# Topographic Data Models",
            "",
            f"Total models: {len(model_items)}",
            "",
            "## Models",
            "",
            *toc_items,
            "",
            *sections,
        ]
    ).rstrip() + "\n"

    output_file.write_text(page, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate HTML and Markdown reference documents from generated topographic data models."
    )
    parser.add_argument(
        "--models-file",
        type=Path,
        default=Path(__file__).resolve().parent / "pydantic_models_classes.py",
        help="Path to a Pydantic models module (for example pydantic_models_classes.py)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parent / "examples" / "topographic_data_models.html",
        help="Output HTML file path",
    )
    parser.add_argument(
        "--markdown-output",
        type=Path,
        default=Path(__file__).resolve().parent / "examples" / "topographic_data_models.md",
        help="Output Markdown file path",
    )

    args = parser.parse_args()
    build_html(args.models_file.resolve(), args.output.resolve())
    build_markdown(args.models_file.resolve(), args.markdown_output.resolve())


if __name__ == "__main__":
    main()
