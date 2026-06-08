import geopandas as gpd
import pytest
from shapely.geometry import Point

from ..config import Source, ThemeDataset
from .transform import normalize_fields


def _gdf(rows: list[dict]) -> gpd.GeoDataFrame:
    """Build a GeoDataFrame with the lifecycle columns normalize_fields expects."""
    for i, row in enumerate(rows):
        row.setdefault("id", f"id-{i}")
        row.setdefault("created_at", None)
        row.setdefault("updated_at", None)
    return gpd.GeoDataFrame(rows, geometry=[Point(0, 0)] * len(rows), crs="EPSG:4326")


def _td(mapping: dict) -> ThemeDataset:
    return ThemeDataset(name="t", source=Source(url="kart@data.koordinates.com:linz/x-topo-150k"), mapping=mapping)


def test_literal_non_string_is_passed_through():
    """Regression: `version: 1` used to crash on int.startswith."""
    out = normalize_fields(_gdf([{}, {}]), _td({"version": 1, "feature_type": "road"}))
    assert out["version"].tolist() == [1, 1]
    assert out["feature_type"].tolist() == ["road", "road"]


def test_column_references():
    gdf = _gdf([{"name": "a", "hway_num": "SH1"}])
    out = normalize_fields(gdf, _td({"name": "$", "highway_number": "$hway_num"}))
    assert out["name"].tolist() == ["a"]
    assert out["highway_number"].tolist() == ["SH1"]


def test_none_source_skips_column():
    out = normalize_fields(_gdf([{"name": "a"}]), _td({"topo_id": None}))
    assert "topo_id" not in out.columns


def test_default_fills_nulls_in_column():
    gdf = _gdf([{"name": "a"}, {"name": None}])
    out = normalize_fields(gdf, _td({"name": {"source": "$", "default": "Unknown"}}))
    assert out["name"].tolist() == ["a", "Unknown"]


def test_absent_column_raises_even_with_default():
    """`default` only fills NULLs in an existing column; an absent column is an error."""
    with pytest.raises(Exception, match="Source column not found"):
        normalize_fields(_gdf([{}]), _td({"name": {"source": "$missing", "default": "Unknown"}}))


def test_missing_column_without_default_raises():
    with pytest.raises(Exception, match="Source column not found"):
        normalize_fields(_gdf([{}]), _td({"name": "$missing"}))
