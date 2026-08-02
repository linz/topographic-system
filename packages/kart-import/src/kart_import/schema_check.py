"""Early, static check of a theme's yml mapping against ``schema/<theme>.json``.

This runs at **config-load time**, to capture obvious authoring mistakes in a theme yml.
It is deliberately *complementary* to the canonical GeoParquet validation that runs in CI.

Rules:
1. **unknown target column**: a mapping key that is not a schema property (schemas are
   ``additionalProperties: false``, so such a column would be rejected at output).
2. **bad literal constant**: a literal mapping value (e.g. ``type: rock``) that does not
   satisfy the property's ``const`` / ``enum`` / ``type``.
3. **null into a non-nullable field** — ``col: null`` where the schema forbids null.
4. **missing required column**: a schema ``required`` property that is neither mapped by the
   theme nor supplied by the pipeline (``PIPELINE_MANAGED``), so the output row would omit it.

A mapping value tagged ``fixup: true`` is skipped for value checks (rules 2/3) as its final
value is only knowable at runtime, but the column still counts as *present* for rule 4.

Schema set selection:
  ``KART_SCHEMA_SET`` = ``current`` (default,``schema/``) or ``next`` (``schema/next/``).
  ``KART_SCHEMA_DIR`` overrides the ``current`` root.

Behaviour is gated by ``KART_SCHEMA_CHECK``:
  ``warn`` (default: log and continue)
  ``strict`` (raise)
  ``off`` (do not check)
"""

from __future__ import annotations

import json
import logging
from functools import cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from .env import env_schema_check_mode, env_schema_dir_override, env_schema_set

logger = logging.getLogger("kart_import")

# packages/kart-import/src/kart_import/schema_check.py -> repo root is five parents up.
REPO_ROOT = Path(__file__).resolve().parents[4]

# Columns supplied by the pipeline itself, not by a theme mapping. These cols are expected to be
# absent from the mapping yet still satisfy a schema ``required`` entry.
#   id / created_at / updated_at  -> transform.normalize_fields / normalize_field_lifecyle
#   geometry                      -> kart export `-lco GEOMETRY_NAME=geometry`
#   bbox                          -> to-parquet ogr2ogr `-lco COVERING_BBOX_NAME=bbox`
PIPELINE_MANAGED = frozenset({"id", "created_at", "updated_at", "geometry", "bbox"})


class SchemaCheckError(Exception):
    """One or more theme configs failed the static schema check (``strict`` mode)."""


def schema_dir(schema_set: str | None = None) -> Path:
    schema_set = schema_set or env_schema_set()
    base = env_schema_dir_override()
    root = Path(base) if base else REPO_ROOT / "schema"
    if schema_set == "next":
        return root / "next"
    if schema_set == "current":
        return root
    raise ValueError(f"Unknown schema_set {schema_set!r}; expected 'current' or 'next'")


def schema_path(theme_name: str, schema_set: str | None = None) -> Path:
    return schema_dir(schema_set) / f"{theme_name}.json"


@cache
def _load_schema(path_str: str) -> dict[str, Any]:
    with open(path_str) as f:
        return json.load(f)


def _is_source_ref(value: Any) -> bool:
    """A ``$`` / ``$col`` reference to a source column — runtime value, not checkable here."""
    return isinstance(value, str) and value.startswith("$")


def check_theme(theme: Any, schema_set: str | None = None) -> list[str]:
    """Return a list of human-readable problems for ``theme``'s mappings."""
    sp = schema_path(theme.name, schema_set)
    if not sp.exists():
        return [f"{theme.name}: no schema at {sp}"]

    doc = _load_schema(str(sp))
    props: dict[str, Any] = doc.get("properties", {})
    required: list[str] = doc.get("required", [])
    defs = doc.get("$defs", {})
    validators = {name: Draft202012Validator({**subschema, "$defs": defs}) for name, subschema in props.items()}
    problems: list[str] = []

    for dataset in theme.datasets:
        specs = dataset.field_specs()

        # Rule 4: every required column must be mapped (even a `fixup`/all-null column counts as
        # present) or supplied by the pipeline; otherwise the emitted row would omit it.
        provided = set(specs) | PIPELINE_MANAGED
        for col in required:
            if col not in provided:
                problems.append(
                    f"{theme.name}/{dataset.name}: missing required column '{col}' "
                    f"(required by schema {theme.name}.json, but not mapped nor pipeline-managed)"
                )

        for target, spec in specs.items():
            if spec.fixup:
                continue
            if target not in props:
                problems.append(
                    f"{theme.name}/{dataset.name}: unknown target column '{target}' "
                    f"(not a property of schema {theme.name}.json)"
                )
                continue
            validator = validators[target]
            # The source is checkable unless it's a `$`-ref (runtime value). An explicit
            # `null` source (`col: null`) IS checked, against nullability.
            to_check: list[Any] = []
            if not _is_source_ref(spec.source):
                to_check.append(spec.source)
            # `default` substitutes on null, so a literal default must satisfy the schema
            # too; a `None` default just means "no default", not "default is null".
            if spec.default is not None and not _is_source_ref(spec.default):
                to_check.append(spec.default)
            for value in to_check:
                errors = list(validator.iter_errors(value))
                if errors:
                    problems.append(f"{theme.name}/{dataset.name}: '{target}: {value!r}' — {errors[0].message}")
    return problems


def check_theme_or_warn(theme: Any) -> list[str]:
    """Config-load hook. ``KART_SCHEMA_CHECK`` = warn (default) | strict | off.

    ``warn`` logs each problem and continues; ``strict`` raises ``SchemaCheckError``;
    ``off`` skips the check entirely.
    """
    mode = env_schema_check_mode()
    if mode == "off":
        return []
    problems = check_theme(theme)
    if problems:
        for p in problems:
            logger.warning("schema-check %s", p)
        # A missing schema file can't be a mapping violation, so it never hard-fails
        # strict mode — only an actual schema breach does.
        if mode == "strict" and schema_path(theme.name).exists():
            raise SchemaCheckError(
                f"{len(problems)} schema problem(s) in theme {theme.name!r}:\n  " + "\n  ".join(problems)
            )
    return problems
