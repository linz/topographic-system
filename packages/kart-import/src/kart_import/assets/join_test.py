import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

from .. import config
from ..config import Join, Lookup, Source, ThemeDataset
from . import prepare, transform

_SRC = Source(url="git@github.com:linz/topographic-source-data", dataset="linz_road_cl")


def test_select_lookup_columns_selects_dedups_and_drops_null_key():
    gdf = gpd.GeoDataFrame(
        {"t50_fid": [1, 2, 2, None], "width": ["a", "b", "c", "d"], "name_id": [10, 20, 30, 40]},
        geometry=[Point(0, 0)] * 4,
        crs="EPSG:4326",
    )
    lookup = Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns=["width"])

    out = prepare.select_lookup_columns(gdf, lookup)

    assert list(out.columns) == ["t50_fid", "width"]  # geometry + unselected dropped
    assert out["t50_fid"].tolist() == [1, 2]  # null key dropped, duplicate t50_fid=2 collapsed
    assert out.loc[out["t50_fid"] == 1, "width"].iloc[0] == "a"  # kept first


def test_select_lookup_columns_raises_on_missing_key():
    gdf = gpd.GeoDataFrame({"width": ["a"]}, geometry=[Point(0, 0)], crs="EPSG:4326")
    lookup = Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns=["width"])

    with pytest.raises(KeyError, match="key column 't50_fid' not found"):
        prepare.select_lookup_columns(gdf, lookup)


def test_select_lookup_columns_raises_on_missing_source_column():
    gdf = gpd.GeoDataFrame({"t50_fid": [1], "width": ["a"]}, geometry=[Point(0, 0)], crs="EPSG:4326")
    lookup = Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns=["missing"])

    with pytest.raises(KeyError, match="source column 'missing' not found"):
        prepare.select_lookup_columns(gdf, lookup)


def test_apply_joins_left_merges_by_key_dtype_robust(tmp_path, monkeypatch):
    lookup = Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns=["width"])
    monkeypatch.setitem(config.LOOKUP_MAP, "road_lkp", lookup)
    monkeypatch.setattr(transform, "WORKING_LOOKUP_DIR", tmp_path / "lookup")
    monkeypatch.setattr(transform, "_resolve_lookup_commit", lambda name, release_id: "abc123")

    (tmp_path / "lookup" / "road_lkp").mkdir(parents=True)
    pd.DataFrame({"t50_fid": [1, 3], "width": ["WIDE", "NARROW"]}).to_parquet(
        tmp_path / "lookup" / "road_lkp" / "abc123.parquet"
    )

    # source t50_fid is float. The join must still match the int lookup key.
    gdf = gpd.GeoDataFrame({"t50_fid": [1.0, 2.0, 3.0]}, geometry=[Point(0, 0)] * 3, crs="EPSG:4326")
    td = ThemeDataset(
        name="ds",
        source=Source(url="kart@data.koordinates.com:linz/x-topo-150k"),
        joins=[Join(lookup="road_lkp", left_on="t50_fid")],
    )

    out = transform.apply_joins(gdf, td, 66)

    assert len(out) == 3  # left join, lookup unique on key -> no fan-out
    assert out.loc[out["t50_fid"] == 1.0, "road_lkp.width"].iloc[0] == "WIDE"
    assert out.loc[out["t50_fid"] == 3.0, "road_lkp.width"].iloc[0] == "NARROW"
    assert out.loc[out["t50_fid"] == 2.0, "road_lkp.width"].isna().all()  # unmatched -> null
    assert isinstance(out, gpd.GeoDataFrame) and out.crs is not None  # stays geo


def test_apply_joins_release_predating_lookup_fills_null(tmp_path, monkeypatch):
    """A release older than the lookup's first commit is not enriched; the columns are added as null."""
    lookup = Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns=["width"])
    monkeypatch.setitem(config.LOOKUP_MAP, "road_lkp", lookup)
    monkeypatch.setattr(transform, "WORKING_LOOKUP_DIR", tmp_path / "lookup")
    monkeypatch.setattr(transform, "_resolve_lookup_commit", lambda name, release_id: None)  # predates lookup

    gdf = gpd.GeoDataFrame({"t50_fid": [1.0, 2.0]}, geometry=[Point(0, 0)] * 2, crs="EPSG:4326")
    td = ThemeDataset(
        name="ds",
        source=Source(url="kart@data.koordinates.com:linz/x-topo-150k"),
        joins=[Join(lookup="road_lkp", left_on="t50_fid")],
    )

    out = transform.apply_joins(gdf, td, 40)

    assert len(out) == 2  # rows untouched
    assert "road_lkp.width" in out.columns  # namespaced column present
    assert out["road_lkp.width"].isna().all()  # ...but null
    assert isinstance(out, gpd.GeoDataFrame) and out.crs is not None


def test_apply_joins_picks_parquet_for_resolved_commit(tmp_path, monkeypatch):
    """Per-release versioning: each release loads the parquet for its resolved commit."""
    lookup = Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns=["width"])
    monkeypatch.setitem(config.LOOKUP_MAP, "road_lkp", lookup)
    monkeypatch.setattr(transform, "WORKING_LOOKUP_DIR", tmp_path / "lookup")
    # release 60 -> older commit, release 66 -> newer commit
    monkeypatch.setattr(transform, "_resolve_lookup_commit", lambda name, release_id: f"commit{release_id}")

    (tmp_path / "lookup" / "road_lkp").mkdir(parents=True)
    pd.DataFrame({"t50_fid": [1], "width": ["OLD"]}).to_parquet(tmp_path / "lookup" / "road_lkp" / "commit60.parquet")
    pd.DataFrame({"t50_fid": [1], "width": ["NEW"]}).to_parquet(tmp_path / "lookup" / "road_lkp" / "commit66.parquet")

    gdf = gpd.GeoDataFrame({"t50_fid": [1.0]}, geometry=[Point(0, 0)], crs="EPSG:4326")
    td = ThemeDataset(
        name="ds",
        source=Source(url="kart@data.koordinates.com:linz/x-topo-150k"),
        joins=[Join(lookup="road_lkp", left_on="t50_fid")],
    )

    assert transform.apply_joins(gdf, td, 60)["road_lkp.width"].iloc[0] == "OLD"
    assert transform.apply_joins(gdf, td, 66)["road_lkp.width"].iloc[0] == "NEW"


def test_resolve_lookup_commit_follows_commit_resolution(monkeypatch):
    """The release->commit gate is purely 'what commit does the lookup have as-of this release'."""
    monkeypatch.setattr(transform, "get_release_commit", lambda repo, until: ("abcdef12", "2026-05-15T00:00:00Z"))
    assert transform._resolve_lookup_commit("road_lkp", 66) == "abcdef12"

    monkeypatch.setattr(transform, "get_release_commit", lambda repo, until: None)
    assert transform._resolve_lookup_commit("road_lkp", 66) is None


def test_prepare_lookup_slims_each_commit_export(tmp_path, monkeypatch):
    """prepare_lookup slims every per-commit export into a parquet named by commit."""
    lookup = Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns=["width"])
    monkeypatch.setitem(config.LOOKUP_MAP, "road_lkp", lookup)
    monkeypatch.setattr(prepare, "WORKING_EXPORTS_DIR", tmp_path / "export")
    monkeypatch.setattr(prepare, "WORKING_LOOKUP_DIR", tmp_path / "lookup")

    export_dir = tmp_path / "export" / "lookup" / "road_lkp"
    export_dir.mkdir(parents=True)
    gpd.GeoDataFrame(
        {"t50_fid": [1, 2], "width": ["WIDE", "NARROW"]},
        geometry=[Point(0, 0)] * 2,
        crs="EPSG:4326",
    ).to_file(export_dir / "abc123.json", driver="GeoJSON")

    out_dir = prepare.prepare_lookup("road_lkp")

    assert out_dir == tmp_path / "lookup" / "road_lkp"  # per-lookup dir, one parquet per commit
    out = pd.read_parquet(out_dir / "abc123.parquet")  # keyed by commit
    assert list(out.columns) == ["t50_fid", "width"]  # slimmed to key + selected columns
    assert sorted(out["width"]) == ["NARROW", "WIDE"]
