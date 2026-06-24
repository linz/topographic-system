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


def test_matches_numeric_column_with_string_key():
    """A column read as int (pyogrio may do this) still matches a YAML string key, for both
    `set`/`where` and `replace`."""
    gdf, td = _td(
        [
            {"column": "lane_count", "set": "single", "where": {"way_count": "1"}},
            {"column": "way_count", "replace": {"1": "one way"}},
        ],
        way_count=[1, 2, 1],  # integer dtype; config keys are strings
        lane_count=["x", "x", "x"],
    )
    out = apply_corrections(gdf, td)
    assert out["lane_count"].tolist() == ["single", "x", "single"]
    assert out["way_count"].tolist() == ["one way", 2, "one way"]


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
