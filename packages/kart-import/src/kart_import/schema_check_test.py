import json

import pytest

from .config import Theme
from .schema_check import (
    RFC3339_STRING,
    SchemaCheckError,
    check_theme,
    check_theme_or_warn,
    schema_dir,
    schema_dtypes,
)

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
    # Point the checker at a temporary schema root holding v0.3/airport.json, the default version.
    (tmp_path / "v0.3").mkdir()
    (tmp_path / "v0.3" / "airport.json").write_text(json.dumps(SCHEMA))
    monkeypatch.setenv("KART_SCHEMA_DIR", str(tmp_path))
    return tmp_path / "v0.3"


def _theme(mapping):
    return Theme.model_validate(
        {
            "name": "airport",
            "target_repo": "topographic-data",
            "target_epsg": "EPSG:4167",
            "schema_version": {"default": "v0.3"},
            "datasets": [{"source": SOURCE, "name": "ds", "mapping": mapping}],
        }
    )


def test_schema_dir_resolves_known_versions(monkeypatch, tmp_path):
    monkeypatch.setenv("KART_SCHEMA_DIR", str(tmp_path))
    assert schema_dir("v0.3") == tmp_path / "v0.3"
    assert schema_dir("v0.4") == tmp_path / "v0.4"


def test_schema_dir_rejects_unknown_version(monkeypatch, tmp_path):
    monkeypatch.setenv("KART_SCHEMA_DIR", str(tmp_path))
    with pytest.raises(ValueError, match="expected one of"):
        schema_dir("v0.9")


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


def test_missing_schema_says_how_to_generate_it(schema_folder):
    theme = _theme({"type": "airport"})
    theme.name = "no_such_theme"
    (problem,) = check_theme(theme)
    assert "npm run -w packages/schema bundle" in problem


def test_strict_mode_raises_on_missing_schema(schema_folder, monkeypatch):
    # Without a schema the coerce pass skips type conversion, so a strict run must not carry on.
    monkeypatch.setenv("KART_SCHEMA_CHECK", "strict")
    theme = _theme({"type": "airport"})
    theme.name = "no_such_theme"
    with pytest.raises(SchemaCheckError, match="no schema"):
        check_theme_or_warn(theme)


def test_warn_mode_allows_missing_schema(schema_folder, monkeypatch):
    # A schema-less dev run is still allowed outside strict mode, with a warning.
    monkeypatch.setenv("KART_SCHEMA_CHECK", "warn")
    theme = _theme({"type": "airport"})
    theme.name = "no_such_theme"
    problems = check_theme_or_warn(theme)
    assert len(problems) == 1 and "no schema" in problems[0]


def test_strict_mode_raises_on_missing_earlier_version_schema(schema_folder, monkeypatch):
    # Mappings are checked against the latest version, but releases pinned to an earlier version
    # are coerced with that version's schema, so its file must exist too.
    monkeypatch.setenv("KART_SCHEMA_CHECK", "strict")
    v04 = schema_folder.parent / "v0.4"
    v04.mkdir()
    (v04 / "airport.json").write_text(json.dumps(SCHEMA))
    (schema_folder / "airport.json").unlink()  # v0.3, still used before release 67
    theme = _theme({"type": "airport"})
    theme.schema_version = {"default": "v0.3", "67": "v0.4"}
    with pytest.raises(SchemaCheckError, match=r"v0\.3/airport\.json"):
        check_theme_or_warn(theme)


def test_check_theme_or_warn_uses_theme_schema_version(schema_folder):
    # Changing the theme's own `schema_version` picks a different subfolder, so a schema only
    # present under that version is found.
    v04 = schema_folder.parent / "v0.4"
    v04.mkdir()
    (v04 / "airport.json").write_text(json.dumps({**SCHEMA, "required": ["type"]}))

    theme = _theme({"name": "$"})  # missing required 'type' -- only checkable if v0.4 is used
    theme.schema_version = {"default": "v0.4"}
    problems = check_theme_or_warn(theme)
    assert any("missing required column 'type'" in p for p in problems)


# A sample schema exercising each shape `schema_dtypes` has to read: a nullable integer, a plain
# number, a `$ref` enum, a date-time string, and the two pipeline-supplied columns that don't
# map to a scalar at all.
DTYPE_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "created_at": {"type": "string", "format": "date-time"},
        "t50_fid": {"anyOf": [{"type": "integer", "minimum": 0}, {"type": "null"}]},
        "height": {"anyOf": [{"type": "number"}, {"type": "null"}]},
        "kind": {"$ref": "#/$defs/kind"},
        "note": {},
        "geometry": {"not": {"type": "null"}},
        "bbox": {"$ref": "#/$defs/BBox"},
    },
    "$defs": {
        "kind": {"type": "string", "enum": ["a", "b"]},
        "BBox": {"type": "object", "properties": {"xmin": {"type": "number"}}},
    },
}


@pytest.fixture
def dtype_schema_folder(tmp_path, monkeypatch):
    (tmp_path / "v0.3").mkdir()
    (tmp_path / "v0.3" / "water_point.json").write_text(json.dumps(DTYPE_SCHEMA))
    monkeypatch.setenv("KART_SCHEMA_DIR", str(tmp_path))
    return tmp_path / "v0.3"


def test_schema_dtypes_reads_each_property_shape(dtype_schema_folder):
    assert schema_dtypes("water_point", "v0.3") == {
        "id": "string",
        "created_at": RFC3339_STRING,
        "t50_fid": "Int32",  # the `null` branch of the anyOf drops out; the dtype is nullable
        "height": "Float64",
        "kind": "string",  # resolved through $defs
    }


def test_schema_dtypes_skips_columns_it_cannot_type(dtype_schema_folder):
    """`geometry` (declared only as not-null), `bbox` (an object) and `note` ({}) are supplied
    by the pipeline or unconstrained: There's no scalar dtype to assert."""
    dtypes = schema_dtypes("water_point", "v0.3")

    assert "geometry" not in dtypes and "bbox" not in dtypes and "note" not in dtypes


def test_schema_dtypes_is_empty_without_a_schema(dtype_schema_folder):
    """Local dev without a schema keeps whatever dtypes the sources carried."""
    assert schema_dtypes("no_such_theme", "v0.3") == {}


# A minimal discriminated union: two `oneOf` branches disagreeing on `subtype` (a real enum in
# `rock`, always-null in `plain`) and a `type` resolved differently in each (`const` vs. `$ref`
# enum) — the same shape as `land_cover_point.json`'s `rock_outcrop` vs. its plain point types.
ONEOF_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "oneOf": [{"$ref": "#/$defs/rock"}, {"$ref": "#/$defs/plain"}],
    "$defs": {
        "rock": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "type": {"type": "string", "const": "rock"},
                "subtype": {"$ref": "#/$defs/rock_subtype"},
                "name": {"anyOf": [{"type": "string"}, {"type": "null"}]},
            },
            "required": ["type", "subtype"],
        },
        "plain": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "type": {"$ref": "#/$defs/plain_type"},
                "subtype": {"type": "null"},
                "name": {"anyOf": [{"type": "string"}, {"type": "null"}]},
            },
            "required": ["type"],
        },
        "rock_subtype": {"type": "string", "enum": ["big", "small"]},
        "plain_type": {"type": "string", "enum": ["cemetery", "swamp"]},
    },
}


@pytest.fixture
def oneof_schema_folder(tmp_path, monkeypatch):
    (tmp_path / "v0.3").mkdir()
    (tmp_path / "v0.3" / "land_cover_point.json").write_text(json.dumps(ONEOF_SCHEMA))
    monkeypatch.setenv("KART_SCHEMA_DIR", str(tmp_path))
    return tmp_path / "v0.3"


def _oneof_theme(mapping):
    return Theme.model_validate(
        {
            "name": "land_cover_point",
            "target_repo": "topographic-data",
            "target_epsg": "EPSG:4167",
            "schema_version": {"default": "v0.3"},
            "datasets": [{"source": SOURCE, "name": "ds", "mapping": mapping}],
        }
    )


def test_oneof_valid_mapping_for_each_branch_has_no_problems(oneof_schema_folder):
    rock = _oneof_theme({"type": "rock", "subtype": "big", "name": "$"})
    plain = _oneof_theme({"type": "cemetery", "subtype": None, "name": "$"})
    assert check_theme(rock) == []
    assert check_theme(plain) == []


def test_oneof_value_checked_against_matched_branch(oneof_schema_folder):
    # `type: cemetery` selects the `plain` branch, whose `subtype` must be null.
    problems = check_theme(_oneof_theme({"type": "cemetery", "subtype": "big"}))
    assert any("'subtype: 'big''" in p for p in problems)


def test_oneof_missing_required_column_for_matched_branch(oneof_schema_folder):
    # `rock` requires `subtype`; a dataset mapped as `type: rock` without it is missing one.
    problems = check_theme(_oneof_theme({"type": "rock"}))
    assert any("missing required column 'subtype'" in p for p in problems)


def test_oneof_unmatched_type_is_reported(oneof_schema_folder):
    problems = check_theme(_oneof_theme({"type": "not_a_real_type"}))
    assert any("cannot resolve a single schema branch" in p for p in problems)


def test_oneof_missing_type_is_reported(oneof_schema_folder):
    problems = check_theme(_oneof_theme({"subtype": "big"}))
    assert any("cannot resolve a single schema branch" in p for p in problems)


def test_oneof_dollar_type_is_reported(oneof_schema_folder):
    # `type` mapped to a `$`-ref is dynamic; the branch can't be resolved statically.
    problems = check_theme(_oneof_theme({"type": "$t"}))
    assert any("cannot resolve a single schema branch" in p for p in problems)


def test_oneof_strict_mode_raises(oneof_schema_folder, monkeypatch):
    monkeypatch.setenv("KART_SCHEMA_CHECK", "strict")
    with pytest.raises(SchemaCheckError, match="schema problem"):
        check_theme_or_warn(_oneof_theme({"type": "not_a_real_type"}))


def test_oneof_dtypes_merge_across_branches(oneof_schema_folder):
    # `subtype`'s always-null `plain` branch drops out, same as a plain nullable `anyOf` does;
    # `type` and `name` agree (as `string`) across both branches.
    assert schema_dtypes("land_cover_point", "v0.3") == {
        "type": "string",
        "subtype": "string",
        "name": "string",
    }


def test_schema_dtypes_resolves_a_nested_pointer(tmp_path, monkeypatch):
    """`$ref` is resolved by the referencing library, so a pointer into a nested property
    works. Reading the last path segment out of `$defs` would find nothing here."""
    (tmp_path / "v0.3").mkdir()
    (tmp_path / "v0.3" / "nested.json").write_text(
        json.dumps(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "properties": {"count": {"$ref": "#/$defs/Wrapper/properties/count"}},
                "$defs": {"Wrapper": {"properties": {"count": {"type": "integer"}}}},
            }
        )
    )
    monkeypatch.setenv("KART_SCHEMA_DIR", str(tmp_path))

    assert schema_dtypes("nested", "v0.3") == {"count": "Int32"}
