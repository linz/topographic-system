from datetime import datetime

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

from ..config import Join, Release, Source, ThemeDataset
from ..kart_types import coerce_integer_columns
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
    out = normalize_fields(_gdf([{}, {}]), _td({"version": 1, "feature_type": "road"}), 1)
    assert out["version"].tolist() == [1, 1]
    assert out["feature_type"].tolist() == ["road", "road"]


def test_column_references():
    gdf = _gdf([{"name": "a", "hway_num": "SH1"}])
    out = normalize_fields(gdf, _td({"name": "$", "highway_number": "$hway_num"}), 1)
    assert out["name"].tolist() == ["a"]
    assert out["highway_number"].tolist() == ["SH1"]


def test_none_source_creates_null_column():
    out = normalize_fields(_gdf([{"name": "a"}, {"name": "b"}]), _td({"topo_id": None}), 1)
    assert out["topo_id"].tolist() == [None, None]


def test_none_source_with_default_fills_every_row():
    out = normalize_fields(_gdf([{"name": "a"}, {"name": "b"}]), _td({"topo_id": {"default": "X"}}), 1)
    assert out["topo_id"].tolist() == ["X", "X"]


def test_default_fills_nulls_in_column():
    gdf = _gdf([{"name": "a"}, {"name": None}])
    out = normalize_fields(gdf, _td({"name": {"source": "$", "default": "Unknown"}}), 1)
    assert out["name"].tolist() == ["a", "Unknown"]


def test_absent_column_raises_even_with_default():
    """`default` only fills NULLs in an existing column; an absent column is an error."""
    with pytest.raises(Exception, match="Source column not found"):
        normalize_fields(_gdf([{}]), _td({"name": {"source": "$missing", "default": "Unknown"}}), 1)


def test_missing_column_without_default_raises():
    with pytest.raises(Exception, match="Source column not found"):
        normalize_fields(_gdf([{}]), _td({"name": "$missing"}), 1)


def _since_td() -> ThemeDataset:
    return _td({"orientation": {"source": "$orientatn", "since_release": 49}})


def test_since_release_nulls_absent_column_in_earlier_release():
    out = normalize_fields(_gdf([{}, {}]), _since_td(), 48)
    assert out["orientation"].tolist() == [None, None]


def test_since_release_still_raises_from_that_release_on():
    """A typo'd name must not silently become an all-NULL column once the source has it."""
    with pytest.raises(Exception, match="Source column not found"):
        normalize_fields(_gdf([{}]), _since_td(), 49)


def test_since_release_maps_present_column_in_earlier_release():
    """Presence wins over the release gate, so a boundary set too late loses no data."""
    out = normalize_fields(_gdf([{"orientatn": 90.0}]), _since_td(), 30)
    assert out["orientation"].tolist() == [90.0]


def test_since_release_honours_default_for_earlier_release():
    spec = {"orientation": {"source": "$orientatn", "since_release": 49, "default": 0}}
    out = normalize_fields(_gdf([{}]), _td(spec), 48)
    assert out["orientation"].tolist() == [0]


@pytest.mark.parametrize(
    "stem, expected",
    [
        ("2026-06-29_" + "a" * 40, "a" * 40),  # what export_dataset_releases names a commit export
        ("b" * 40, "b" * 40),  # bare commit, as export_lookup names them
        ("empty", None),  # an export not built from a commit -> nothing to look a schema up by
        ("2026-06-29_notasha", None),
    ],
)
def test_source_commit_reads_the_commit_off_the_export_name(tmp_path, stem, expected):
    f = tmp_path / f"{stem}.gpkg"
    f.write_text("")
    assert transform._source_commit(f) == expected


def test_source_commit_follows_the_release_symlink(tmp_path):
    """Releases point at a shared, commit-named export; the commit is on the target, not the link."""
    target = tmp_path / f"2026-06-29_{'c' * 40}.gpkg"
    target.write_text("")
    link = tmp_path / "nz_road_centrelines.gpkg"
    link.symlink_to(target)

    assert transform._source_commit(link) == "c" * 40


# `fid_lifecycle` keys its map on `str(fid)` of the value `kart diff -o json-lines` emits, so an
# integer pk keys as "3197173". `normalize_field_lifecyle` re-derives that with `.astype(str)`,
# which makes the lookup depend on the pk's dtype - the one place restoring declared types is
# load-bearing beyond the joins.
LIFECYCLE = {"3197173": {"id": "uuid-a", "created_at": "2024-01-01T00:00:00Z"}}
PK_SCHEMA = [{"name": "t50_fid", "dataType": "numeric", "precision": 10, "scale": 0}]
_TD = ThemeDataset(name="t", source=Source(url="kart@data.koordinates.com:linz/x-topo-150k"))


def _pk_frame(dtype):
    return gpd.GeoDataFrame({"t50_fid": pd.Series([3197173], dtype=dtype)}, geometry=[Point(0, 0)], crs="EPSG:4167")


def test_lifecycle_pk_matches_once_the_declared_type_is_restored():
    gdf = coerce_integer_columns(_pk_frame("float64"), PK_SCHEMA, context="ctx")

    out = transform.normalize_field_lifecyle(gdf, _TD, LIFECYCLE)

    assert out["id"].tolist() == ["uuid-a"]


def test_lifecycle_pk_misses_while_the_export_float_survives():
    """Pins why the restoration matters here: float64 keys as '3197173.0' and matches nothing."""
    with pytest.raises(KeyError, match=r"3197173\.0"):
        transform.normalize_field_lifecyle(_pk_frame("float64"), _TD, LIFECYCLE)


def test_empty_transform_carries_the_target_columns_and_crs():
    """An empty source has no columns to normalise, so the pipeline emits the shape the
    normalisers would have produced and lets the emptiness flow through to the theme."""
    td = ThemeDataset(
        name="ds",
        source=Source(url="kart@data.koordinates.com:linz/x-topo-150k"),
        mapping={"t50_fid": "$", "type": "trig"},
    )

    out = transform.empty_transform(td, "EPSG:4167")

    assert out.empty
    assert list(out.columns) == ["id", "created_at", "updated_at", "t50_fid", "type", "geometry"]
    assert out.crs == "EPSG:4167"
