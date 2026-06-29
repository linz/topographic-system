import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

from . import config, joins
from .assets import prepare
from .config import Join, Lookup, Source, ThemeDataset

_SRC = Source(url="git@github.com:linz/topographic-source-data", dataset="linz_road_cl")


@pytest.fixture(autouse=True)
def _clear_resolve_lookup_cache():
    """_resolve_lookup_commit is @functools.cache'd; clear it between tests so a value cached by
    one test (keyed on lookup_name/release_id) can't bleed into another and mask a real resolve."""
    if hasattr(joins._resolve_lookup_commit, "cache_clear"):
        joins._resolve_lookup_commit.cache_clear()
    yield


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


def test_join_fingerprint_reflects_lookup_commits(monkeypatch):
    td = ThemeDataset(
        name="ds",
        source=Source(url="kart@data.koordinates.com:linz/x-topo-150k"),
        joins=[Join(lookup="road_lkp", left_on="t50_fid")],
    )
    # lookup advances per release -> fingerprints differ -> transforms must not be shared
    monkeypatch.setattr(joins, "_resolve_lookup_commit", lambda name, release_id: f"commit{release_id}")
    assert joins.join_fingerprint(td, 66) == joins.join_fingerprint(td, 66)
    assert joins.join_fingerprint(td, 60) != joins.join_fingerprint(td, 66)

    # lookup frozen across releases -> identical fingerprints -> one transform reusable
    monkeypatch.setattr(joins, "_resolve_lookup_commit", lambda name, release_id: "frozen")
    assert joins.join_fingerprint(td, 60) == joins.join_fingerprint(td, 66)


def test_join_fingerprint_empty_without_joins():
    td = ThemeDataset(name="ds", source=Source(url="kart@data.koordinates.com:linz/x-topo-150k"))
    assert joins.join_fingerprint(td, 1) == ()
    assert joins.join_fingerprint(td, 2) == ()


def test_apply_joins_left_merges_on_matching_key_type(tmp_path, monkeypatch):
    lookup = Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns=["width"])
    monkeypatch.setitem(config.LOOKUP_MAP, "road_lkp", lookup)
    monkeypatch.setattr(joins, "WORKING_LOOKUP_DIR", tmp_path / "lookup")
    monkeypatch.setattr(joins, "_resolve_lookup_commit", lambda name, release_id: "abc123")

    (tmp_path / "lookup" / "road_lkp").mkdir(parents=True)
    pd.DataFrame({"t50_fid": [1, 3], "width": ["WIDE", "NARROW"]}).to_parquet(
        tmp_path / "lookup" / "road_lkp" / "abc123.parquet"
    )

    # source t50_fid is the same (integer) type as the lookup key, so the join matches.
    gdf = gpd.GeoDataFrame({"t50_fid": [1, 2, 3]}, geometry=[Point(0, 0)] * 3, crs="EPSG:4326")
    td = ThemeDataset(
        name="ds",
        source=Source(url="kart@data.koordinates.com:linz/x-topo-150k"),
        joins=[Join(lookup="road_lkp", left_on="t50_fid")],
    )

    out = joins.apply_joins(gdf, td, 66)

    assert len(out) == 3  # left join, lookup unique on key -> no fan-out
    assert out.loc[out["t50_fid"] == 1, "road_lkp.width"].iloc[0] == "WIDE"
    assert out.loc[out["t50_fid"] == 3, "road_lkp.width"].iloc[0] == "NARROW"
    assert out.loc[out["t50_fid"] == 2, "road_lkp.width"].isna().all()  # unmatched -> null
    assert isinstance(out, gpd.GeoDataFrame) and out.crs is not None  # stays geo


def test_apply_joins_matches_leading_zero_string_keys_exactly(tmp_path, monkeypatch):
    """Strings are strings: '01' matches its own row, not '1'"""
    lookup = Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns=["width"])
    monkeypatch.setitem(config.LOOKUP_MAP, "road_lkp", lookup)
    monkeypatch.setattr(joins, "WORKING_LOOKUP_DIR", tmp_path / "lookup")
    monkeypatch.setattr(joins, "_resolve_lookup_commit", lambda name, release_id: "abc123")

    (tmp_path / "lookup" / "road_lkp").mkdir(parents=True)
    pd.DataFrame({"t50_fid": ["01", "1"], "width": ["ZERO_ONE", "ONE"]}).to_parquet(
        tmp_path / "lookup" / "road_lkp" / "abc123.parquet"
    )

    gdf = gpd.GeoDataFrame({"t50_fid": ["01", "1", "02"]}, geometry=[Point(0, 0)] * 3, crs="EPSG:4326")
    td = ThemeDataset(
        name="ds",
        source=Source(url="kart@data.koordinates.com:linz/x-topo-150k"),
        joins=[Join(lookup="road_lkp", left_on="t50_fid")],
    )

    out = joins.apply_joins(gdf, td, 66)

    assert out.loc[out["t50_fid"] == "01", "road_lkp.width"].iloc[0] == "ZERO_ONE"
    assert out.loc[out["t50_fid"] == "1", "road_lkp.width"].iloc[0] == "ONE"
    assert out.loc[out["t50_fid"] == "02", "road_lkp.width"].isna().all()


def test_apply_joins_raises_on_key_type_mismatch(tmp_path, monkeypatch):
    """Joining keys of different types is a config/data error, not silently coerced."""
    lookup = Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns=["width"])
    monkeypatch.setitem(config.LOOKUP_MAP, "road_lkp", lookup)
    monkeypatch.setattr(joins, "WORKING_LOOKUP_DIR", tmp_path / "lookup")
    monkeypatch.setattr(joins, "_resolve_lookup_commit", lambda name, release_id: "abc123")

    (tmp_path / "lookup" / "road_lkp").mkdir(parents=True)
    pd.DataFrame({"t50_fid": [1, 3], "width": ["WIDE", "NARROW"]}).to_parquet(
        tmp_path / "lookup" / "road_lkp" / "abc123.parquet"
    )

    # source t50_fid is float; lookup key is integer -> mismatch -> raise.
    gdf = gpd.GeoDataFrame({"t50_fid": [1.0, 2.0, 3.0]}, geometry=[Point(0, 0)] * 3, crs="EPSG:4326")
    td = ThemeDataset(
        name="ds",
        source=Source(url="kart@data.koordinates.com:linz/x-topo-150k"),
        joins=[Join(lookup="road_lkp", left_on="t50_fid")],
    )

    with pytest.raises(TypeError, match="join key type mismatch"):
        joins.apply_joins(gdf, td, 66)


def test_apply_joins_empty_lookup_matches_without_raising(tmp_path, monkeypatch):
    """A lookup with no rows for this release (e.g. no data yet) must not trip the type guard:
    the join still runs and the lookup columns come through null."""
    lookup = Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns=["width"])
    monkeypatch.setitem(config.LOOKUP_MAP, "road_lkp", lookup)
    monkeypatch.setattr(joins, "WORKING_LOOKUP_DIR", tmp_path / "lookup")
    monkeypatch.setattr(joins, "_resolve_lookup_commit", lambda name, release_id: "abc123")

    (tmp_path / "lookup" / "road_lkp").mkdir(parents=True)
    # Empty parquet whose key dtype (object) even *differs* from the source (int): still must not raise,
    # because an empty side has nothing to join and pandas merges it cleanly regardless of dtype.
    pd.DataFrame({"t50_fid": pd.Series([], dtype="object"), "width": pd.Series([], dtype="object")}).to_parquet(
        tmp_path / "lookup" / "road_lkp" / "abc123.parquet"
    )

    gdf = gpd.GeoDataFrame({"t50_fid": [1, 2, 3]}, geometry=[Point(0, 0)] * 3, crs="EPSG:4326")
    td = ThemeDataset(
        name="ds",
        source=Source(url="kart@data.koordinates.com:linz/x-topo-150k"),
        joins=[Join(lookup="road_lkp", left_on="t50_fid")],
    )

    joins.validate_join_key_types(gdf, td, 66)  # pre-flight is also tolerant of the empty key
    out = joins.apply_joins(gdf, td, 66)

    assert len(out) == 3  # rows preserved
    assert "road_lkp.width" in out.columns
    assert out["road_lkp.width"].isna().all()  # nothing to match -> null
    assert isinstance(out, gpd.GeoDataFrame) and out.crs is not None


def test_validate_join_key_types_raises_on_mismatch(tmp_path, monkeypatch):
    """Pre-flight catches the same type mismatch apply_joins would, but before any merge runs."""
    lookup = Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns=["width"])
    monkeypatch.setitem(config.LOOKUP_MAP, "road_lkp", lookup)
    monkeypatch.setattr(joins, "WORKING_LOOKUP_DIR", tmp_path / "lookup")
    monkeypatch.setattr(joins, "_resolve_lookup_commit", lambda name, release_id: "abc123")

    (tmp_path / "lookup" / "road_lkp").mkdir(parents=True)
    pd.DataFrame({"t50_fid": [1, 3], "width": ["WIDE", "NARROW"]}).to_parquet(
        tmp_path / "lookup" / "road_lkp" / "abc123.parquet"
    )

    gdf = gpd.GeoDataFrame({"t50_fid": [1.0, 2.0]}, geometry=[Point(0, 0)] * 2, crs="EPSG:4326")  # float vs int
    td = ThemeDataset(
        name="ds",
        source=Source(url="kart@data.koordinates.com:linz/x-topo-150k"),
        joins=[Join(lookup="road_lkp", left_on="t50_fid")],
    )

    with pytest.raises(TypeError, match="join key type mismatch"):
        joins.validate_join_key_types(gdf, td, 66)


def test_validate_join_key_types_skips_release_predating_lookup(monkeypatch):
    """A release with no lookup commit has no key to compare, so pre-flight skips it (no file access)."""
    lookup = Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns=["width"])
    monkeypatch.setitem(config.LOOKUP_MAP, "road_lkp", lookup)
    monkeypatch.setattr(joins, "_resolve_lookup_commit", lambda name, release_id: None)

    gdf = gpd.GeoDataFrame({"t50_fid": [1.0, 2.0]}, geometry=[Point(0, 0)] * 2, crs="EPSG:4326")
    td = ThemeDataset(
        name="ds",
        source=Source(url="kart@data.koordinates.com:linz/x-topo-150k"),
        joins=[Join(lookup="road_lkp", left_on="t50_fid")],
    )

    joins.validate_join_key_types(gdf, td, 40)  # no raise, no lookup dir needed


def test_apply_joins_release_predating_lookup_fills_null(tmp_path, monkeypatch):
    """A release older than the lookup's first commit is not enriched; the columns are added as null."""
    lookup = Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns=["width"])
    monkeypatch.setitem(config.LOOKUP_MAP, "road_lkp", lookup)
    monkeypatch.setattr(joins, "WORKING_LOOKUP_DIR", tmp_path / "lookup")
    monkeypatch.setattr(joins, "_resolve_lookup_commit", lambda name, release_id: None)  # predates lookup

    gdf = gpd.GeoDataFrame({"t50_fid": [1.0, 2.0]}, geometry=[Point(0, 0)] * 2, crs="EPSG:4326")
    td = ThemeDataset(
        name="ds",
        source=Source(url="kart@data.koordinates.com:linz/x-topo-150k"),
        joins=[Join(lookup="road_lkp", left_on="t50_fid")],
    )

    out = joins.apply_joins(gdf, td, 40)

    assert len(out) == 2  # rows untouched
    assert "road_lkp.width" in out.columns  # namespaced column present
    assert out["road_lkp.width"].isna().all()  # ...but null
    assert isinstance(out, gpd.GeoDataFrame) and out.crs is not None


def test_apply_joins_picks_parquet_for_resolved_commit(tmp_path, monkeypatch):
    """Per-release versioning: each release loads the parquet for its resolved commit."""
    lookup = Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns=["width"])
    monkeypatch.setitem(config.LOOKUP_MAP, "road_lkp", lookup)
    monkeypatch.setattr(joins, "WORKING_LOOKUP_DIR", tmp_path / "lookup")
    # release 60 -> older commit, release 66 -> newer commit
    monkeypatch.setattr(joins, "_resolve_lookup_commit", lambda name, release_id: f"commit{release_id}")

    (tmp_path / "lookup" / "road_lkp").mkdir(parents=True)
    pd.DataFrame({"t50_fid": [1], "width": ["OLD"]}).to_parquet(tmp_path / "lookup" / "road_lkp" / "commit60.parquet")
    pd.DataFrame({"t50_fid": [1], "width": ["NEW"]}).to_parquet(tmp_path / "lookup" / "road_lkp" / "commit66.parquet")

    gdf = gpd.GeoDataFrame({"t50_fid": [1]}, geometry=[Point(0, 0)], crs="EPSG:4326")
    td = ThemeDataset(
        name="ds",
        source=Source(url="kart@data.koordinates.com:linz/x-topo-150k"),
        joins=[Join(lookup="road_lkp", left_on="t50_fid")],
    )

    assert joins.apply_joins(gdf, td, 60)["road_lkp.width"].iloc[0] == "OLD"
    assert joins.apply_joins(gdf, td, 66)["road_lkp.width"].iloc[0] == "NEW"


def test_resolve_lookup_commit_follows_commit_resolution(tmp_path, monkeypatch):
    """The release->commit gate is purely 'what commit does the lookup have as-of this release'."""
    monkeypatch.setattr(joins, "SOURCE_DIR", tmp_path)
    (tmp_path / "road_lkp").mkdir()

    monkeypatch.setattr(joins, "get_release_commit", lambda repo, until: ("abcdef12", "2026-05-15T00:00:00Z"))
    assert joins._resolve_lookup_commit("road_lkp", 66) == "abcdef12"

    # release predates the lookup: no commit as-of the release, but the repo HAS history.
    joins._resolve_lookup_commit.cache_clear()  # re-resolving the same pair under a new git state
    monkeypatch.setattr(
        joins, "get_release_commit", lambda repo, until: None if until else ("head0001", "2026-01-01T00:00:00Z")
    )
    assert joins._resolve_lookup_commit("road_lkp", 66) is None


def test_resolve_lookup_commit_errors_on_missing_clone(tmp_path, monkeypatch):
    """A missing source clone must raise, not silently resolve to None (which nulls the join)."""
    monkeypatch.setattr(joins, "SOURCE_DIR", tmp_path)  # road_lkp dir absent
    with pytest.raises(FileNotFoundError, match="source repo not found"):
        joins._resolve_lookup_commit("road_lkp", 66)


def test_resolve_lookup_commit_errors_on_empty_clone(tmp_path, monkeypatch):
    """A present-but-empty/broken clone (unable to read commit history) must raise, not null the join."""
    monkeypatch.setattr(joins, "SOURCE_DIR", tmp_path)
    (tmp_path / "road_lkp").mkdir()
    monkeypatch.setattr(joins, "get_release_commit", lambda repo, until: None)  # no commits at all
    with pytest.raises(FileNotFoundError, match="no resolvable commits"):
        joins._resolve_lookup_commit("road_lkp", 66)


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
