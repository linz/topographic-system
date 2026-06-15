import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

from .. import config
from ..config import Join, Lookup, Source, ThemeDataset
from . import prepare, transform

_SRC = Source(url="git@github.com:linz/topographic-source-data", dataset="linz_road_cl")


def test_select_lookup_columns_renames_dedups_and_drops_null_key():
    gdf = gpd.GeoDataFrame(
        {"t50_fid": [1, 2, 2, None], "width": ["a", "b", "c", "d"], "name_id": [10, 20, 30, 40]},
        geometry=[Point(0, 0)] * 4,
        crs="EPSG:4326",
    )
    lookup = Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns={"width_indicator": "$width"})

    out = prepare.select_lookup_columns(gdf, lookup)

    assert list(out.columns) == ["t50_fid", "width_indicator"]  # geometry + unselected dropped
    assert out["t50_fid"].tolist() == [1, 2]  # null key dropped, duplicate t50_fid=2 collapsed
    assert out.loc[out["t50_fid"] == 1, "width_indicator"].iloc[0] == "a"  # kept first


def test_select_lookup_columns_dollar_shorthand_uses_target_name_as_source():
    gdf = gpd.GeoDataFrame(
        {"t50_fid": [1, 2], "width": ["a", "b"]},
        geometry=[Point(0, 0)] * 2,
        crs="EPSG:4326",
    )
    # "$" means: source column == target column name.
    lookup = Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns={"width": "$"})

    out = prepare.select_lookup_columns(gdf, lookup)

    assert out["width"].tolist() == ["a", "b"]


def test_select_lookup_columns_raises_on_missing_key():
    gdf = gpd.GeoDataFrame({"width": ["a"]}, geometry=[Point(0, 0)], crs="EPSG:4326")
    lookup = Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns={"width_indicator": "$width"})

    with pytest.raises(KeyError, match="key column 't50_fid' not found"):
        prepare.select_lookup_columns(gdf, lookup)


def test_select_lookup_columns_raises_on_non_dollar_expr():
    gdf = gpd.GeoDataFrame({"t50_fid": [1], "width": ["a"]}, geometry=[Point(0, 0)], crs="EPSG:4326")
    lookup = Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns={"width_indicator": "width"})

    with pytest.raises(ValueError, match="must reference a source column"):
        prepare.select_lookup_columns(gdf, lookup)


def test_select_lookup_columns_raises_on_missing_source_column():
    gdf = gpd.GeoDataFrame({"t50_fid": [1], "width": ["a"]}, geometry=[Point(0, 0)], crs="EPSG:4326")
    lookup = Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns={"width_indicator": "$missing"})

    with pytest.raises(KeyError, match="source column 'missing' not found"):
        prepare.select_lookup_columns(gdf, lookup)


def test_apply_joins_left_merges_by_key_dtype_robust(tmp_path, monkeypatch):
    lookup = Lookup(name="road_lkp", source=_SRC, key="t50_fid", columns={"width_indicator": "$width"})
    monkeypatch.setitem(config.LOOKUP_MAP, "road_lkp", lookup)
    monkeypatch.setattr(transform, "WORKING_LOOKUP_DIR", tmp_path / "lookup")

    lookup_dir = tmp_path / "lookup" / "release_66"
    lookup_dir.mkdir(parents=True)
    pd.DataFrame({"t50_fid": [1, 3], "width_indicator": ["WIDE", "NARROW"]}).to_parquet(lookup_dir / "road_lkp.parquet")

    # source t50_fid is float. The join must still match the int lookup key.
    gdf = gpd.GeoDataFrame({"t50_fid": [1.0, 2.0, 3.0]}, geometry=[Point(0, 0)] * 3, crs="EPSG:4326")
    td = ThemeDataset(
        name="ds",
        source=Source(url="kart@data.koordinates.com:linz/x-topo-150k"),
        joins=[Join(lookup="road_lkp", left_on="t50_fid")],
    )

    out = transform.apply_joins(gdf, td, 66)

    assert len(out) == 3  # left join, lookup unique on key -> no fan-out
    assert out.loc[out["t50_fid"] == 1.0, "width_indicator"].iloc[0] == "WIDE"
    assert out.loc[out["t50_fid"] == 3.0, "width_indicator"].iloc[0] == "NARROW"
    assert out.loc[out["t50_fid"] == 2.0, "width_indicator"].isna().all()  # unmatched -> null
    assert isinstance(out, gpd.GeoDataFrame) and out.crs is not None  # stays geo
