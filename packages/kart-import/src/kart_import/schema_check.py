"""Early, static check of a theme's yml mapping against ``schema/<theme>.json``.

This runs at **config-load time**, to capture obvious authoring mistakes in a theme yml.
It is deliberately *complementary* to the canonical GeoParquet validation that runs in CI.

Rules:
1. **unknown target column**: a mapping key that is not a schema property (schemas are
   ``additionalProperties: false``, so such a column would be rejected at output).
2. **bad literal constant**: a literal mapping value (e.g. ``type: rock``) that does not
   satisfy the property's ``const`` / ``enum`` / ``type``.
3. **null into a non-nullable field** — ``col: null`` where the schema forbids null.

A mapping value tagged ``fixup: true`` is skipped as its final value will be determined
by a dataset fixup and only knowable at runtime.

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

from .env import env_schema_check_mode, env_schema_dir_override, env_schema_set

logger = logging.getLogger("kart_import")

# packages/kart-import/src/kart_import/schema_check.py -> repo root is five parents up.
REPO_ROOT = Path(__file__).resolve().parents[4]

# Columns supplied by the pipeline itself, not by a theme mapping.
PIPELINE_MANAGED = frozenset({"id", "created_at", "updated_at", "metadata", "geometry", "bbox"})


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


def _resolve(prop: dict[str, Any], doc: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten a property schema to its leaf branches, following local ``$ref`` and
    ``anyOf`` / ``oneOf``. Unhandled composites (e.g. ``allOf``) are returned as-is, which
    is the safe direction: an unrecognised branch is treated as unconstrained."""
    ref = prop.get("$ref")
    if ref:
        if not ref.startswith("#/$defs/"):
            return [prop]  # external/unknown ref — don't guess, treat as unconstrained
        target = doc.get("$defs", {}).get(ref.split("/")[-1])
        return _resolve(target, doc) if target is not None else [prop]
    for key in ("anyOf", "oneOf"):
        if key in prop:
            branches: list[dict[str, Any]] = []
            for sub in prop[key]:
                branches.extend(_resolve(sub, doc))
            return branches
    return [prop]


def _null_ok(branch: dict[str, Any]) -> bool:
    if branch.get("type") == "null":
        return True
    if "const" in branch:
        return branch["const"] is None
    if "enum" in branch:
        return None in branch["enum"]
    return "type" not in branch  # {} or a type-less branch accepts null


def _value_ok(value: Any, branch: dict[str, Any]) -> bool:
    if "const" in branch:
        return value == branch["const"]
    if "enum" in branch:
        return value in branch["enum"]
    t = branch.get("type")
    if t is None:
        return True  # unconstrained
    if t == "null":
        return value is None
    if t == "string":
        return isinstance(value, str)
    if t == "boolean":
        return isinstance(value, bool)
    if t == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if t == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    return True


def _literal_ok(value: Any, branches: list[dict[str, Any]]) -> bool:
    check = _null_ok if value is None else lambda b: _value_ok(value, b)
    return any(check(b) for b in branches)


def _allowed(branches: list[dict[str, Any]]) -> list[Any] | None:
    """The enumerated values a property accepts, for a friendlier error message."""
    for b in branches:
        if "const" in b:
            return [b["const"]]
        if "enum" in b:
            return b["enum"]
    return None


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
    problems: list[str] = []

    for dataset in theme.datasets:
        for target, spec in dataset.field_specs().items():
            if spec.fixup:
                continue
            if target not in props:
                problems.append(
                    f"{theme.name}/{dataset.name}: unknown target column '{target}' "
                    f"(not a property of schema {theme.name}.json)"
                )
                continue
            branches = _resolve(props[target], doc)
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
                if not _literal_ok(value, branches):
                    allowed = _allowed(branches)
                    hint = f" allowed: {allowed}" if allowed else ""
                    problems.append(f"{theme.name}/{dataset.name}: '{target}: {value!r}' does not satisfy schema{hint}")
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
