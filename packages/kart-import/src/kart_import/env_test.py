import pytest

from .env import (
    env_push_force,
    env_push_to_master,
    env_schema_check_mode,
    env_schema_dir_override,
    env_schema_set,
    env_transform_format,
)


def test_transform_format_defaults_to_parquet(monkeypatch):
    monkeypatch.delenv("KART_TRANSFORM_FORMAT", raising=False)
    assert env_transform_format() == "parquet"


def test_transform_format_override_is_case_insensitive(monkeypatch):
    monkeypatch.setenv("KART_TRANSFORM_FORMAT", "GeoJSON")
    assert env_transform_format() == "geojson"


def test_transform_format_rejects_unknown_value(monkeypatch):
    monkeypatch.setenv("KART_TRANSFORM_FORMAT", "shapefile")
    with pytest.raises(ValueError, match="parquet"):
        env_transform_format()


@pytest.mark.parametrize("env_fn,var", [(env_push_to_master, "KART_PUSH_MASTER"), (env_push_force, "KART_PUSH_FORCE")])
def test_push_flag_defaults_to_false(monkeypatch, env_fn, var):
    monkeypatch.delenv(var, raising=False)
    assert env_fn() is False


@pytest.mark.parametrize("env_fn,var", [(env_push_to_master, "KART_PUSH_MASTER"), (env_push_force, "KART_PUSH_FORCE")])
def test_push_flag_is_case_insensitive_true(monkeypatch, env_fn, var):
    monkeypatch.setenv(var, "True")
    assert env_fn() is True


@pytest.mark.parametrize("env_fn,var", [(env_push_to_master, "KART_PUSH_MASTER"), (env_push_force, "KART_PUSH_FORCE")])
def test_push_flag_non_true_is_false(monkeypatch, env_fn, var):
    monkeypatch.setenv(var, "1")  # only "true" enables the flag
    assert env_fn() is False


def test_schema_set_defaults_to_current(monkeypatch):
    monkeypatch.delenv("KART_SCHEMA_SET", raising=False)
    assert env_schema_set() == "current"
    monkeypatch.setenv("KART_SCHEMA_SET", "next")
    assert env_schema_set() == "next"


def test_schema_check_mode_defaults_and_is_case_insensitive(monkeypatch):
    monkeypatch.delenv("KART_SCHEMA_CHECK", raising=False)
    assert env_schema_check_mode() == "warn"
    monkeypatch.setenv("KART_SCHEMA_CHECK", "STRICT")
    assert env_schema_check_mode() == "strict"


def test_schema_check_mode_rejects_unknown_value(monkeypatch):
    monkeypatch.setenv("KART_SCHEMA_CHECK", "loud")
    with pytest.raises(ValueError, match="warn"):
        env_schema_check_mode()


def test_schema_dir_override_unset_is_none(monkeypatch):
    monkeypatch.delenv("KART_SCHEMA_DIR", raising=False)
    assert env_schema_dir_override() is None


def test_schema_dir_override_returns_existing_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("KART_SCHEMA_DIR", str(tmp_path))
    assert env_schema_dir_override() == str(tmp_path)


def test_schema_dir_override_rejects_missing_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("KART_SCHEMA_DIR", str(tmp_path / "does-not-exist"))
    with pytest.raises(FileNotFoundError, match="KART_SCHEMA_DIR"):
        env_schema_dir_override()
