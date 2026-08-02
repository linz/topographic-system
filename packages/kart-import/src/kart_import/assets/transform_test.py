from datetime import datetime

import geopandas as gpd
import pytest
from shapely.geometry import Point

from ..config import Join, Release, Source, ThemeDataset
from . import transform
from .transform import find_canonical_release, normalize_fields


def _releases(*ids: int) -> list[Release]:
    return [Release(id=i, date=datetime(2020, 1, 1)) for i in ids]


def _link_shared_source(root, dataset_name: str, release_ids: list[int]) -> None:
    """Point each release's export at one shared commit file (what export's symlinking produces)."""
    commit_file = root / f"{dataset_name}_commit.json"
    commit_file.write_text("{}")
    for r in release_ids:
        d = root / f"release_{r}"
        d.mkdir()
        (d / f"{dataset_name}.json").symlink_to(commit_file)


def test_find_canonical_release_dedups_on_source_and_joins(tmp_path, monkeypatch):
    monkeypatch.setattr(transform, "WORKING_EXPORTS_DIR", tmp_path)
    releases = _releases(1, 2, 3)
    _link_shared_source(tmp_path, "ds", [1, 2, 3])  # all three share one source export
    td = ThemeDataset(
        name="ds",
        source=Source(url="kart@data.koordinates.com:linz/x-topo-150k"),
        joins=[Join(lookup="road_lkp", left_on="t50_fid")],
    )

    # lookup frozen across releases -> earliest release is canonical for all
    monkeypatch.setattr(transform, "join_fingerprint", lambda td, rid: ("frozen",))
    assert find_canonical_release("ds", td, 3, releases) == 1

    # lookup advanced for release 3 -> release 3 transforms on its own; 1 and 2 still share
    monkeypatch.setattr(transform, "join_fingerprint", lambda td, rid: ("old",) if rid < 3 else ("new",))
    assert find_canonical_release("ds", td, 3, releases) == 3
    assert find_canonical_release("ds", td, 2, releases) == 1


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


def test_none_source_creates_null_column():
    out = normalize_fields(_gdf([{"name": "a"}, {"name": "b"}]), _td({"topo_id": None}))
    assert out["topo_id"].tolist() == [None, None]


def test_none_source_with_default_fills_every_row():
    out = normalize_fields(_gdf([{"name": "a"}, {"name": "b"}]), _td({"topo_id": {"default": "X"}}))
    assert out["topo_id"].tolist() == ["X", "X"]


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
