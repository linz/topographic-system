import json

import pytest

from .config import Theme
from .schema_check import SchemaCheckError, check_theme, check_theme_or_warn, schema_dir

SOURCE = "kart@data.koordinates.com:linz/nz-airport-polygons-topo-150k"

# A minimal but representative schema: a const-constrained `type`, a nullable-via-anyOf
# `name`, a `$ref` enum `kind`, a non-nullable number `elevation`, and an unconstrained
# `note` ({} accepts anything, including null).
SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "id": {"type": "string"},
        "type": {"type": "string", "const": "airport"},
        "name": {"anyOf": [{"type": "string"}, {"type": "null"}]},
        "kind": {"$ref": "#/$defs/kind"},
        "elevation": {"type": "number"},
        "note": {},
    },
    "$defs": {"kind": {"type": "string", "enum": ["testing", "other_test"]}},
}


@pytest.fixture
def schema_folder(tmp_path, monkeypatch):
    # Point the checker at a temporary `current` schema root holding airport.json.
    (tmp_path / "airport.json").write_text(json.dumps(SCHEMA))
    monkeypatch.setenv("KART_SCHEMA_DIR", str(tmp_path))
    monkeypatch.delenv("KART_SCHEMA_SET", raising=False)
    return tmp_path


def _theme(mapping):
    return Theme.model_validate(
        {
            "name": "airport",
            "target_repo": "topographic-data",
            "target_epsg": "EPSG:4167",
            "datasets": [{"source": SOURCE, "name": "ds", "mapping": mapping}],
        }
    )


def test_schema_dir_resolves_current_and_next(monkeypatch, tmp_path):
    monkeypatch.setenv("KART_SCHEMA_DIR", str(tmp_path))
    monkeypatch.delenv("KART_SCHEMA_SET", raising=False)
    assert schema_dir() == tmp_path
    assert schema_dir("next") == tmp_path / "next"


def test_schema_dir_rejects_unknown_set(monkeypatch, tmp_path):
    monkeypatch.setenv("KART_SCHEMA_DIR", str(tmp_path))
    with pytest.raises(ValueError, match="expected 'current' or 'next'"):
        schema_dir("future")


def test_valid_mapping_has_no_problems(schema_folder):
    theme = _theme({"type": "airport", "name": "$", "kind": "testing", "elevation": "$elev"})
    assert check_theme(theme) == []


def test_unknown_target_column_is_reported(schema_folder):
    problems = check_theme(_theme({"type": "airport", "surface": "sealed"}))
    assert any("unknown target column 'surface'" in p for p in problems)


def test_const_violation_is_reported(schema_folder):
    problems = check_theme(_theme({"type": "aerodrome"}))
    assert any("'type: 'aerodrome''" in p for p in problems)


def test_enum_violation_reports_allowed_values(schema_folder):
    problems = check_theme(_theme({"kind": "test"}))
    assert len(problems) == 1
    assert "['testing', 'other_test']" in problems[0]


def test_source_ref_is_not_checked(schema_folder):
    # `$`/`$col` values are runtime-only; the static check must never flag them.
    assert check_theme(_theme({"type": "airport", "kind": "$k", "elevation": "$e"})) == []


def test_null_into_non_nullable_is_reported(schema_folder):
    problems = check_theme(_theme({"elevation": None}))
    assert any("'elevation: None'" in p for p in problems)


def test_null_into_nullable_is_allowed(schema_folder):
    # `name` is anyOf[string, null] and `note` is {}. Both accept an explicit null.
    assert check_theme(_theme({"name": None, "note": None})) == []


def test_fixup_tagged_column_is_not_checked(schema_folder):
    # A `fixup: true` column is produced/consumed by a fixup at runtime, so none of the
    # three rules apply: neither a null into the non-nullable `elevation` nor a `surface`
    # that isn't a schema property is flagged.
    theme = _theme(
        {
            "type": "airport",
            "elevation": {"fixup": True},
            "surface": {"source": "$s", "fixup": True},
        }
    )
    assert check_theme(theme) == []


def test_literal_default_is_checked(schema_folder):
    # source is a runtime ref, but the literal default must still satisfy the schema.
    problems = check_theme(_theme({"kind": {"source": "$k", "default": "commercial"}}))
    assert any("'kind: 'commercial''" in p for p in problems)


def test_missing_required_column_is_reported(schema_folder):
    # `type` is required but neither mapped nor pipeline-managed -> flagged.
    (schema_folder / "airport.json").write_text(json.dumps({**SCHEMA, "required": ["id", "type", "name"]}))
    problems = check_theme(_theme({"name": "$"}))
    assert any("missing required column 'type'" in p for p in problems)


def test_pipeline_managed_required_column_is_not_reported(schema_folder):
    # `id` is required but supplied by the pipeline, so it must never be flagged as missing.
    (schema_folder / "airport.json").write_text(json.dumps({**SCHEMA, "required": ["id", "type"]}))
    problems = check_theme(_theme({"type": "airport"}))
    assert not any("missing required column 'id'" in p for p in problems)


def test_fixup_column_satisfies_required(schema_folder):
    # A `fixup: true` column has no checkable value but the column IS present at output.
    (schema_folder / "airport.json").write_text(json.dumps({**SCHEMA, "required": ["type"]}))
    assert check_theme(_theme({"type": {"source": "$t", "fixup": True}})) == []


def test_missing_schema_is_reported(schema_folder):
    theme = _theme({"type": "airport"})
    theme.name = "no_such_theme"
    problems = check_theme(theme)
    assert len(problems) == 1 and "no schema" in problems[0]


def test_strict_mode_raises(schema_folder, monkeypatch):
    monkeypatch.setenv("KART_SCHEMA_CHECK", "strict")
    with pytest.raises(SchemaCheckError, match="schema problem"):
        check_theme_or_warn(_theme({"type": "aerodrome"}))


def test_warn_mode_returns_problems_without_raising(schema_folder, monkeypatch):
    monkeypatch.setenv("KART_SCHEMA_CHECK", "warn")
    problems = check_theme_or_warn(_theme({"type": "aerodrome"}))
    assert problems and all("aerodrome" in p for p in problems)


def test_off_mode_skips(schema_folder, monkeypatch):
    monkeypatch.setenv("KART_SCHEMA_CHECK", "off")
    assert check_theme_or_warn(_theme({"type": "aerodrome"})) == []


def test_strict_mode_does_not_raise_on_missing_schema(schema_folder, monkeypatch):
    # A theme with no schema file creates a warning even in strict mode.
    monkeypatch.setenv("KART_SCHEMA_CHECK", "strict")
    theme = _theme({"type": "airport"})
    theme.name = "no_such_theme"
    problems = check_theme_or_warn(theme)
    assert len(problems) == 1 and "no schema" in problems[0]
