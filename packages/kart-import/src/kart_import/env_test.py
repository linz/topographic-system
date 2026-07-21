import pytest

from .env import env_push_force, env_push_to_master, env_transform_format


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
