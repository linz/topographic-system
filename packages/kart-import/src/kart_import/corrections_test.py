import geopandas as gpd
import pytest
from shapely.geometry import Point

from .config import ThemeDataset
from .corrections import apply_corrections


def _td(corrections: list[dict], **cols) -> tuple[gpd.GeoDataFrame, ThemeDataset]:
    """Build a (gdf, dataset) pair: `cols` are column->values, geometry is filler."""
    n = len(next(iter(cols.values())))
    gdf = gpd.GeoDataFrame(cols, geometry=[Point(i, i) for i in range(n)], crs="EPSG:4326")
    td = ThemeDataset.model_validate(
        {"name": "t", "source": "kart@data.koordinates.com:linz/x-topo-150k", "corrections": corrections}
    )
    return gdf, td


def test_replace_single_pair():
    gdf, td = _td([{"column": "tunnel_use2", "replace": {"ivestock": "livestock"}}], tunnel_use2=["ivestock", "cattle"])
    out = apply_corrections(gdf, td)
    assert out["tunnel_use2"].tolist() == ["livestock", "cattle"]


def test_replace_multiple_pairs_in_one_column():
    gdf, td = _td([{"column": "c", "replace": {"a": "x", "b": "y"}}], c=["a", "b", "c"])
    out = apply_corrections(gdf, td)
    assert out["c"].tolist() == ["x", "y", "c"]


def test_same_column_corrected_by_two_entries():
    gdf, td = _td(
        [
            {"column": "c", "replace": {"a": "b"}},
            {"column": "c", "replace": {"b": "z"}},
        ],
        c=["a", "b"],
    )
    out = apply_corrections(gdf, td)
    # both rows end at "z": row0 a->b then b->z; row1 b->z
    assert out["c"].tolist() == ["z", "z"]


def test_set_where_on_another_column():
    gdf, td = _td(
        [{"column": "support_type", "set": "pole", "where": {"type": "telephone"}}],
        support_type=["unknown", "unknown"],
        type=["telephone", "power"],
    )
    out = apply_corrections(gdf, td)
    assert out["support_type"].tolist() == ["pole", "unknown"]


def test_tunnel_rule_ordering():
    """Rule 2 (set tunnel_use where tunnel_use2='vehicle') must read tunnel_use2 before
    rule 3 overwrites it to 'livestock'."""
    gdf, td = _td(
        [
            {"column": "tunnel_use", "set": "vehicle", "where": {"tunnel_use2": "vehicle"}},
            {"column": "tunnel_use2", "set": "livestock", "where": {"tunnel_use2": "vehicle"}},
        ],
        tunnel_use=["foot", "foot"],
        tunnel_use2=["vehicle", "rail"],
    )
    out = apply_corrections(gdf, td)
    assert out["tunnel_use"].tolist() == ["vehicle", "foot"]
    assert out["tunnel_use2"].tolist() == ["livestock", "rail"]


def test_matches_int_column_with_int_key():
    """Keys are matched on their raw value: an int column matches an int YAML key, for both
    `set`/`where` and `replace`."""
    gdf, td = _td(
        [
            {"column": "lane_count", "set": "single", "where": {"way_count": 1}},
            {"column": "way_count", "replace": {1: "one way"}},
        ],
        way_count=[1, 2, 1],  # integer dtype; config keys are ints to match
        lane_count=["x", "x", "x"],
    )
    out = apply_corrections(gdf, td)
    assert out["lane_count"].tolist() == ["single", "x", "single"]
    assert out["way_count"].tolist() == ["one way", 2, "one way"]


def test_type_mismatch_between_key_and_column_raises():
    """A string config key against an int column is a config error, not a silent no-match."""
    gdf, td = _td([{"column": "way_count", "replace": {"1": "one way"}}], way_count=[1, 2, 1])
    with pytest.raises(TypeError, match="type mismatch.*way_count.*integer.*'1' is string"):
        apply_corrections(gdf, td)


def test_float_key_matches_float_column():
    """When nulls widen an int column to float, a float YAML key (`1.0`, not `1`) matches it.
    This is the supported way to correct a float-read column under the strict type rule."""
    gdf, td = _td([{"column": "way_count", "replace": {1.0: "one way"}}], way_count=[1.0, 2.0, 1.0])
    out = apply_corrections(gdf, td)
    assert out["way_count"].tolist() == ["one way", 2.0, "one way"]


def test_int_key_against_float_column_raises():
    """An int key (`1`) does not silently match a float column - the author must use `1.0`."""
    gdf, td = _td([{"column": "way_count", "replace": {1: "one way"}}], way_count=[1.0, 2.0, 1.0])
    with pytest.raises(TypeError, match="type mismatch.*way_count.*float.*config value 1 is integer"):
        apply_corrections(gdf, td)


def test_where_type_mismatch_raises():
    gdf, td = _td(
        [{"column": "lane_count", "set": "single", "where": {"way_count": "1"}}],
        way_count=[1, 2, 1],
        lane_count=["x", "x", "x"],
    )
    with pytest.raises(TypeError, match="type mismatch.*way_count.*integer.*'1' is string"):
        apply_corrections(gdf, td)


def test_set_null_on_multi_condition_where():
    """BM-1741 comment: structure_point.structure_type -> NULL where structure_type='uncovered'
    AND type='tank'. Only the row matching both conditions is nulled."""
    gdf, td = _td(
        [{"column": "structure_type", "set": None, "where": {"structure_type": "uncovered", "type": "tank"}}],
        structure_type=["uncovered", "uncovered", "covered"],
        type=["tank", "pipe", "tank"],
    )
    out = apply_corrections(gdf, td)
    assert out["structure_type"].isna().tolist() == [True, False, False]
    assert out["structure_type"].tolist()[1:] == ["uncovered", "covered"]


def test_missing_target_column_raises():
    gdf, td = _td([{"column": "absent", "replace": {"a": "b"}}], present=["a"])
    with pytest.raises(ValueError, match="correction column.*absent.*in t"):
        apply_corrections(gdf, td)


def test_missing_where_column_raises():
    gdf, td = _td([{"column": "c", "set": "x", "where": {"absent": "y"}}], c=["a"])
    with pytest.raises(ValueError, match="correction column.*absent.*in t"):
        apply_corrections(gdf, td)


def test_bm_1741_corrections_end_to_end():
    """All BM-1741 corrections (minus trig_type, which is a mapping literal) on one frame."""
    gdf, td = _td(
        [
            {"column": "tunnel_use2", "replace": {"ivestock": "livestock"}},
            {"column": "tunnel_use", "set": "vehicle", "where": {"tunnel_use2": "vehicle"}},
            {"column": "tunnel_use2", "set": "livestock", "where": {"tunnel_use2": "vehicle"}},
            {"column": "way_count", "replace": {"1": "one way"}},
            {"column": "road_access", "replace": {"m": "mp"}},
            {"column": "support_type", "set": "pole", "where": {"type": "telephone"}},
        ],
        tunnel_use=["foot", "foot", "foot"],
        tunnel_use2=["ivestock", "vehicle", "rail"],
        way_count=["1", "2", "1"],
        road_access=["m", "p", "m"],
        support_type=["x", "x", "x"],
        type=["telephone", "power", "telephone"],
    )
    out = apply_corrections(gdf, td)
    assert out["tunnel_use2"].tolist() == ["livestock", "livestock", "rail"]
    assert out["tunnel_use"].tolist() == ["foot", "vehicle", "foot"]
    assert out["way_count"].tolist() == ["one way", "2", "one way"]
    assert out["road_access"].tolist() == ["mp", "p", "mp"]
    assert out["support_type"].tolist() == ["pole", "x", "pole"]
