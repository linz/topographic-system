from types import SimpleNamespace

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

from . import config, joins
from .assets import prepare
from .config import Join, Lookup, Source, ThemeDataset

_SRC = Source(url="git@github.com:linz/topographic-source-data", dataset="linz_road_cl")


def _gdf(fids) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame({"t50_fid": fids}, geometry=[Point(0, 0)] * len(fids), crs="EPSG:4326")


def _td() -> ThemeDataset:
    return ThemeDataset(
        name="ds",
        source=Source(url="kart@data.koordinates.com:linz/x-topo-150k"),
        joins=[Join(lookup="road_lkp", left_on="t50_fid")],
    )


@pytest.fixture(autouse=True)
def _clear_resolve_lookup_cache():
    """_resolve_lookup_commit is @functools.cache'd; clear it between tests so a value cached by
    one test (keyed on lookup_name/release_id) can't bleed into another and mask a real resolve."""
    joins._resolve_lookup_commit.cache_clear()
    yield


@pytest.fixture
def env(tmp_path, monkeypatch):
    """A registered 'road_lkp' lookup with an isolated WORKING_LOOKUP_DIR, plus helpers:
    `resolves_to(commit)` fixes the release->commit resolution (None => release predates the lookup),
    and `write(df, commit)` drops a prepared per-commit parquet."""
    lookup = Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns=["width"])
    monkeypatch.setitem(config.LOOKUP_MAP, "road_lkp", lookup)
    monkeypatch.setattr(joins, "WORKING_LOOKUP_DIR", tmp_path)
    (tmp_path / "road_lkp").mkdir()

    def resolves_to(commit):
        monkeypatch.setattr(joins, "_resolve_lookup_commit", lambda *_: commit)

    def write(df, commit="abc123"):
        df.to_parquet(tmp_path / "road_lkp" / f"{commit}.parquet")

    return SimpleNamespace(td=_td(), resolves_to=resolves_to, write=write)

def test_select_lookup_columns_selects_dedups_and_drops_null_key():
    gdf = gpd.GeoDataFrame(
        {"t50_fid": [1, 2, 2, None], "width": ["a", "b", "c", "d"], "name_id": [10, 20, 30, 40]},
        geometry=[Point(0, 0)] * 4,
        crs="EPSG:4326",
    )
    out = prepare.select_lookup_columns(gdf, Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns=["width"]))

    assert list(out.columns) == ["t50_fid", "width"]  # geometry + unselected dropped
    assert out["t50_fid"].tolist() == [1, 2]  # null key dropped, duplicate t50_fid=2 collapsed
    assert out.loc[out["t50_fid"] == 1, "width"].iloc[0] == "a"  # kept first


@pytest.mark.parametrize(
    "columns, cols, match",
    [
        (["width"], {"width": ["a"]}, "key column 't50_fid' not found"),
        (["missing"], {"t50_fid": [1], "width": ["a"]}, "source column 'missing' not found"),
    ],
    ids=["missing-key", "missing-source-column"],
)
def test_select_lookup_columns_raises(columns, cols, match):
    gdf = gpd.GeoDataFrame(cols, geometry=[Point(0, 0)] * len(next(iter(cols.values()))), crs="EPSG:4326")
    with pytest.raises(KeyError, match=match):
        prepare.select_lookup_columns(gdf, Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns=columns))

def test_join_fingerprint_reflects_lookup_commits(monkeypatch):
    td = _td()
    monkeypatch.setattr(joins, "_resolve_lookup_commit", lambda name, release_id: f"commit{release_id}")
    assert joins.join_fingerprint(td, 60) != joins.join_fingerprint(td, 66)  # advances -> not shared
    monkeypatch.setattr(joins, "_resolve_lookup_commit", lambda name, release_id: "frozen")
    assert joins.join_fingerprint(td, 60) == joins.join_fingerprint(td, 66)  # frozen -> one transform reusable


def test_join_fingerprint_empty_without_joins():
    td = ThemeDataset(name="ds", source=Source(url="kart@data.koordinates.com:linz/x-topo-150k"))
    assert joins.join_fingerprint(td, 1) == ()


def test_apply_joins_left_merges_on_matching_key_type(env):
    env.resolves_to("abc123")
    env.write(pd.DataFrame({"t50_fid": [1, 3], "width": ["WIDE", "NARROW"]}))  # int key, same type as source

    out = joins.apply_joins(_gdf([1, 2, 3]), env.td, 66)

    assert len(out) == 3  # left join, lookup unique on key -> no fan-out
    assert out.loc[out["t50_fid"] == 1, "road_lkp.width"].iloc[0] == "WIDE"
    assert out.loc[out["t50_fid"] == 3, "road_lkp.width"].iloc[0] == "NARROW"
    assert out.loc[out["t50_fid"] == 2, "road_lkp.width"].isna().all()  # unmatched -> null
    assert isinstance(out, gpd.GeoDataFrame) and out.crs is not None  # stays geo


def test_apply_joins_matches_leading_zero_string_keys_exactly(env):
    """Strings are strings: '01' matches its own row, not '1'."""
    env.resolves_to("abc123")
    env.write(pd.DataFrame({"t50_fid": ["01", "1"], "width": ["ZERO_ONE", "ONE"]}))

    out = joins.apply_joins(_gdf(["01", "1", "02"]), env.td, 66)

    assert out.loc[out["t50_fid"] == "01", "road_lkp.width"].iloc[0] == "ZERO_ONE"
    assert out.loc[out["t50_fid"] == "1", "road_lkp.width"].iloc[0] == "ONE"
    assert out.loc[out["t50_fid"] == "02", "road_lkp.width"].isna().all()


def test_apply_joins_raises_on_key_type_mismatch(env):
    """Joining keys of different types is a config/data error."""
    env.resolves_to("abc123")
    env.write(pd.DataFrame({"t50_fid": [1, 3], "width": ["WIDE", "NARROW"]}))  # int key
    with pytest.raises(TypeError, match="join key type mismatch"):
        joins.apply_joins(_gdf([1.0, 2.0, 3.0]), env.td, 66)  # float source -> mismatch


def _predates(env):
    env.resolves_to(None)  # no commit as-of this release; no parquet needed


def _empty_lookup(env):
    env.resolves_to("abc123")
    # Empty parquet whose key dtype (object) even differs from the source (int): still must not raise,
    # because an empty side has nothing to join and pandas merges it cleanly regardless of dtype.
    env.write(pd.DataFrame({"t50_fid": pd.Series([], dtype="object"), "width": pd.Series([], dtype="object")}))


@pytest.mark.parametrize("setup", [_predates, _empty_lookup], ids=["release-predates-lookup", "empty-lookup"])
def test_apply_joins_yields_null_columns_when_nothing_to_join(env, setup):
    """A release predating the lookup, or a lookup with no rows, still adds the namespaced column but fills it null.
    Should not trip the key-type guard."""
    setup(env)
    out = joins.apply_joins(_gdf([1, 2, 3]), env.td, 66)

    assert len(out) == 3  # rows preserved
    assert out["road_lkp.width"].isna().all()  # nothing to match -> null
    assert isinstance(out, gpd.GeoDataFrame) and out.crs is not None


def test_apply_joins_picks_parquet_for_resolved_commit(env, monkeypatch):
    """Per-release versioning: each release loads the parquet for its resolved commit."""
    monkeypatch.setattr(joins, "_resolve_lookup_commit", lambda name, release_id: f"commit{release_id}")
    env.write(pd.DataFrame({"t50_fid": [1], "width": ["OLD"]}), commit="commit60")
    env.write(pd.DataFrame({"t50_fid": [1], "width": ["NEW"]}), commit="commit66")

    assert joins.apply_joins(_gdf([1]), env.td, 60)["road_lkp.width"].iloc[0] == "OLD"
    assert joins.apply_joins(_gdf([1]), env.td, 66)["road_lkp.width"].iloc[0] == "NEW"


def test_resolve_lookup_commit_follows_commit_resolution(tmp_path, monkeypatch):
    """The release->commit gate is purely 'what commit does the lookup have as-of this release'."""
    monkeypatch.setattr(joins, "SOURCE_DIR", tmp_path)
    (tmp_path / "road_lkp").mkdir()

    monkeypatch.setattr(joins, "get_release_commit", lambda repo, until: ("abcdef12", "2026-05-15T00:00:00Z"))
    assert joins._resolve_lookup_commit("road_lkp", 66) == "abcdef12"

    # release predates the lookup: no commit as-of the release, but the repo HAS history -> None.
    joins._resolve_lookup_commit.cache_clear()
    monkeypatch.setattr(
        joins, "get_release_commit", lambda repo, until: None if until else ("head0001", "2026-01-01T00:00:00Z")
    )
    assert joins._resolve_lookup_commit("road_lkp", 66) is None


@pytest.mark.parametrize(
    "make_clone, get_commit, match",
    [
        (False, None, "source repo not found"),  # missing clone must raise, not null the join
        (True, lambda repo, until: None, "no resolvable commits"),  # present but no history at all
    ],
    ids=["missing-clone", "empty-clone"],
)
def test_resolve_lookup_commit_errors_on_bad_clone(tmp_path, monkeypatch, make_clone, get_commit, match):
    monkeypatch.setattr(joins, "SOURCE_DIR", tmp_path)
    if make_clone:
        (tmp_path / "road_lkp").mkdir()
        monkeypatch.setattr(joins, "get_release_commit", get_commit)
    with pytest.raises(FileNotFoundError, match=match):
        joins._resolve_lookup_commit("road_lkp", 66)


def test_prepare_lookup_slims_each_commit_export(tmp_path, monkeypatch):
    """prepare_lookup slims every per-commit export into a parquet named by commit."""
    monkeypatch.setitem(
        config.LOOKUP_MAP, "road_lkp", Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns=["width"])
    )
    monkeypatch.setattr(prepare, "WORKING_EXPORTS_DIR", tmp_path / "export")
    monkeypatch.setattr(prepare, "WORKING_LOOKUP_DIR", tmp_path / "lookup")

    export_dir = tmp_path / "export" / "lookup" / "road_lkp"
    export_dir.mkdir(parents=True)
    gpd.GeoDataFrame(
        {"t50_fid": [1, 2], "width": ["WIDE", "NARROW"]}, geometry=[Point(0, 0)] * 2, crs="EPSG:4326"
    ).to_file(export_dir / "abc123.json", driver="GeoJSON")

    out_dir = prepare.prepare_lookup("road_lkp")

    assert out_dir == tmp_path / "lookup" / "road_lkp"  # per-lookup dir, one parquet per commit
    out = pd.read_parquet(out_dir / "abc123.parquet")  # keyed by commit
    assert list(out.columns) == ["t50_fid", "width"]  # slimmed to key + selected columns
    assert sorted(out["width"]) == ["NARROW", "WIDE"]
