import pytest

from ..schema_check import SchemaCheckError
from .schema_check_theme import schema_check_theme

MODULE = "kart_import.assets.schema_check_theme"


@pytest.fixture
def check_dir(tmp_path, monkeypatch):
    out = tmp_path / "schema_check"
    monkeypatch.setattr(f"{MODULE}.WORKING_SCHEMA_CHECK_DIR", out)
    monkeypatch.setattr(f"{MODULE}.get_theme_by_name", lambda name: name)
    return out


def test_writes_sentinel_when_check_passes(check_dir, monkeypatch):
    monkeypatch.setattr(f"{MODULE}.check_theme_or_warn", lambda theme: [])
    schema_check_theme("airport")
    assert (check_dir / "airport.checked").read_text() == "0 problem(s)\n"


def test_writes_sentinel_with_problem_count_in_warn_mode(check_dir, monkeypatch):
    # warn mode: problems are logged by `check_theme_or_warn` and the build carries on
    monkeypatch.setattr(f"{MODULE}.check_theme_or_warn", lambda theme: ["a", "b"])
    schema_check_theme("airport")
    assert (check_dir / "airport.checked").read_text() == "2 problem(s)\n"


def test_no_sentinel_when_strict_check_fails(check_dir, monkeypatch):
    # strict mode: the error must fail the job, and no sentinel may mark the theme as checked
    def fail(theme):
        raise SchemaCheckError("1 schema problem(s) in theme 'airport'")

    monkeypatch.setattr(f"{MODULE}.check_theme_or_warn", fail)
    with pytest.raises(SchemaCheckError):
        schema_check_theme("airport")
    assert not (check_dir / "airport.checked").exists()


def test_checks_the_named_theme(check_dir, monkeypatch):
    checked = []
    monkeypatch.setattr(f"{MODULE}.check_theme_or_warn", lambda theme: checked.append(theme) or [])
    schema_check_theme("water")
    assert checked == ["water"]
