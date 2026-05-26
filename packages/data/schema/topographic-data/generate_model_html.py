from __future__ import annotations

import argparse
import html
import importlib.util
from pathlib import Path
from typing import Any

from pydantic import BaseModel


def _load_pydantic_models_module(models_file: Path):
    # Read file with UTF-8 BOM handling
    content = models_file.read_text(encoding="utf-8-sig")
    
    spec = importlib.util.spec_from_file_location("pydantic_models", models_file)
    if spec is None or spec.loader is None:
      raise RuntimeError(f"Unable to load module from {models_file}")

    module = importlib.util.module_from_spec(spec)
    
    # Execute module with content that has BOM removed
    exec(compile(content, str(models_file), 'exec'), module.__dict__)
    return module


def _json_type_to_text(property_schema: dict[str, Any]) -> str:
    if "type" in property_schema:
        return str(property_schema["type"])

    any_of = property_schema.get("anyOf")
    if isinstance(any_of, list):
        types: list[str] = []
        for item in any_of:
            item_type = item.get("type") if isinstance(item, dict) else None
            if item_type and item_type != "null":
                types.append(str(item_type))
        if types:
            return " | ".join(types)

    return "unknown"


def _render_model_section(model_name: str, model_class: type[BaseModel]) -> str:
    doc = (model_class.__doc__ or "").strip()
    model_schema = model_class.model_json_schema()
    required = set(model_schema.get("required", []))
    properties = model_schema.get("properties", {})
    rows: list[str] = []

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
        extras_text = "; ".join(extra_parts)

        rows.append(
            "<tr>"
            f"<td>{html.escape(field_name)}</td>"
            f"<td>{html.escape(field_type)}</td>"
            f"<td>{html.escape(required_text)}</td>"
            f"<td>{html.escape(default_text)}</td>"
            f"<td>{html.escape(max_length_text)}</td>"
            f"<td>{html.escape(description)}</td>"
            f"<td>{html.escape(extras_text)}</td>"
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


def build_html(models_file: Path, output_file: Path) -> None:
    module = _load_pydantic_models_module(models_file)

    models_by_title = getattr(module, "MODELS_BY_TITLE", None)
    if not isinstance(models_by_title, dict):
        raise RuntimeError("MODELS_BY_TITLE was not found in the loaded module")

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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate an HTML reference document from generated topographic data models."
    )
    parser.add_argument(
        "--models-file",
        type=Path,
        default=Path(__file__).resolve().parent / "pydantic_models.py",
        help="Path to the pydantic_models.py file",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parent / "examples" / "topographic_data_models.html",
        help="Output HTML file path",
    )

    args = parser.parse_args()
    build_html(args.models_file.resolve(), args.output.resolve())


if __name__ == "__main__":
    main()
