import pytest

from .env import env_transform_format


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
