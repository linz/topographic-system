import json
import os
from datetime import UTC, datetime

import geopandas as gpd
import numpy as np
import pytest
from shapely.geometry import LineString, Point, Polygon
from shapely.geometry.base import BaseGeometry
from shapely.wkt import loads

from . import fixups
from .assets import fid_lifecycle, transform
from .assets.transform import apply_fixups
from .config import Release, Theme, ThemeDataset
from .fixups import _drop_listed_empty, drop_degenerate_fences, drop_empty_residential_areas


def _gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame({"name": ["Broken", "OK"]}, geometry=[Point(0, 0), Point(1, 1)], crs="EPSG:4326")


def _td(fixups_cfg: list[dict], name: str = "t") -> ThemeDataset:
    return ThemeDataset.model_validate(
        {"name": name, "source": "kart@data.koordinates.com:linz/x-topo-150k", "fixups": fixups_cfg}
    )


def test_apply_fixups_runs_for_matching_release(monkeypatch):
    def fix(gdf, td, release_id):
        gdf = gdf.copy()
        gdf.loc[gdf["name"] == "Broken", "name"] = f"Fixed-{release_id}"
        return gdf

    monkeypatch.setitem(fixups.FIXUPS, "fix", fix)
    out = apply_fixups(_gdf(), _td([{"fn": "fix", "releases": [64]}]), 64)
    assert out["name"].tolist() == ["Fixed-64", "OK"]


def test_apply_fixups_skips_non_matching_release(monkeypatch):
    def fix(gdf, td, release_id):
        raise AssertionError("fixup must not run for a non-listed release")

    monkeypatch.setitem(fixups.FIXUPS, "fix", fix)
    out = apply_fixups(_gdf(), _td([{"fn": "fix", "releases": [64]}]), 65)
    assert out["name"].tolist() == ["Broken", "OK"]


def test_apply_fixups_applies_to_all_releases_when_unset(monkeypatch):
    seen: list[int] = []

    def fix(gdf, td, release_id):
        seen.append(release_id)
        return gdf

    monkeypatch.setitem(fixups.FIXUPS, "fix", fix)
    apply_fixups(_gdf(), _td([{"fn": "fix"}]), 99)
    assert seen == [99]


LINE_EMPTY = loads("LINESTRING EMPTY")
LINE_REAL = LineString([(174.0, -41.0), (174.1, -41.1)])
POLY_EMPTY = loads("POLYGON EMPTY")
POLY_REAL = Polygon([(175.0, -39.0), (175.1, -39.0), (175.1, -39.1), (175.0, -39.0)])
DEGENERATE_FENCE_FIDS = [7640059, 7640098, 7704786, 7704787]
FENCES = "nz_fence_centrelines"
RESIDENTIAL = "nz_residential_area_polygons"

DS = "ds"
LISTED = {1, 2}


def _fid_gdf(rows: list[tuple[float, BaseGeometry | None]]) -> gpd.GeoDataFrame:
    """A (t50_fid, geometry) frame, the only two columns the drop fixups look at.

    A `None` geometry is a case under test (it fails the FlatGeobuf write like an empty one does).
    An object array rather than a plain list because that is how geopandas holds missing geometry,
    and the only geometry-sequence form the stubs admit a `None` into.
    """
    return gpd.GeoDataFrame(
        {"t50_fid": [fid for fid, _ in rows]},
        geometry=np.array([geom for _, geom in rows], dtype=object),
        crs="EPSG:4167",
    )


@pytest.mark.parametrize(
    "rows, survivors",
    [
        # The gate: a listed fid repaired upstream is kept rather than dropped by a stale list.
        ([(1, LINE_REAL), (9, LINE_REAL)], [1, 9]),
        # Deliberately narrow: an unlisted empty geometry is left to fail loudly at the
        # FlatGeobuf write rather than being silently swallowed here.
        ([(9, LINE_EMPTY), (8, LINE_REAL)], [9, 8]),
        # The plain drop path, on one listed fid, is `test_drop_listed_empty_drops_every_missing_form`.
        ([(1, LINE_EMPTY), (2, LINE_EMPTY), (9, LINE_REAL)], [9]),
        ([(8, LINE_REAL), (9, LINE_REAL)], [8, 9]),
    ],
    ids=[
        "listed-but-repaired-kept",
        "unlisted-empty-kept",
        "every-listed-dropped",
        "nothing-listed-is-a-noop",
    ],
)
def test_drop_listed_empty(rows, survivors):
    out = _drop_listed_empty(_fid_gdf(rows), _td([], DS), DS, LISTED)
    assert out["t50_fid"].tolist() == survivors
    # theme.py concatenates with ignore_index, but a gappy index would still surface in any
    # positional lookup downstream.
    assert out.index.tolist() == list(range(len(survivors)))


@pytest.mark.parametrize("geom", [LINE_EMPTY, POLY_EMPTY, None], ids=["empty-line", "empty-polygon", "null"])
def test_drop_listed_empty_drops_every_missing_form(geom):
    """All three reach the FlatGeobuf write as a NULL geometry, whatever the feature type, so the
    fixups need no per-geometry handling. `None` is what a source publishing a null produces;
    the empties are what `set_precision` leaves behind when a feature rounds away."""
    out = _drop_listed_empty(_fid_gdf([(1, geom), (9, LINE_REAL)]), _td([], DS), DS, LISTED)
    assert out["t50_fid"].tolist() == [9]


@pytest.mark.parametrize("cast", [int, float], ids=["int", "float"])
def test_drop_listed_empty_matches_either_fid_dtype(cast):
    """pyogrio may read an integer t50_fid as float; matching must work for both."""
    out = _drop_listed_empty(_fid_gdf([(cast(1), LINE_EMPTY), (cast(9), LINE_REAL)]), _td([], DS), DS, LISTED)
    assert out["t50_fid"].tolist() == [cast(9)]


def test_drop_listed_empty_rejects_wrong_dataset():
    """A fid list matches nothing on another dataset, so a miswired fixup would otherwise pass as
    a successful no-op build."""
    with pytest.raises(ValueError, match=f"fixup for dataset '{DS}' applied to 'other_dataset'"):
        _drop_listed_empty(_fid_gdf([(9, LINE_REAL)]), _td([], "other_dataset"), DS, LISTED)


def test_drop_degenerate_fences_targets_its_own_fids():
    """Wiring, the one thing the generic tests above cannot check: that this fixup hands
    `_drop_listed_empty` the hand-checked fids and the dataset they were checked against."""
    rows = [(fid, LINE_EMPTY) for fid in DEGENERATE_FENCE_FIDS] + [(123, LINE_REAL)]
    out = drop_degenerate_fences(_fid_gdf(rows), _td([], FENCES), 60)
    assert out["t50_fid"].tolist() == [123]


def test_drop_empty_residential_areas_targets_its_own_fid():
    """As above. `None` is the form release 51 actually ships for residential area 6753838."""
    out = drop_empty_residential_areas(_fid_gdf([(6753838, None), (123, POLY_REAL)]), _td([], RESIDENTIAL), 51)
    assert out["t50_fid"].tolist() == [123]


ROADS = "nz_road_centrelines"
RELEASE_66 = Release(id=66, date=datetime(2024, 5, 16, 21, 4, 22, tzinfo=UTC))
STAMP_66 = "2024-05-16T21:04:22Z"


def _road_gdf(sufis: list[int], names: list[str | None] | None = None) -> gpd.GeoDataFrame:
    """A post-`normalize_fields` road frame: `metadata` already holds the raw `$rna_sufi` values,
    which is the only input `build_road_metadata` has (see its docstring)."""
    return gpd.GeoDataFrame(
        {"metadata": sufis, "name": names if names is not None else [f"ROAD {s}" for s in sufis]},
        geometry=[LineString([(174.0, -41.0), (174.1, -41.1)]) for _ in sufis],
        crs="EPSG:4167",
    )


@pytest.fixture
def road_releases(monkeypatch):
    from . import config as config_module

    monkeypatch.setattr(config_module, "get_releases", lambda: [RELEASE_66])


def test_build_road_metadata_record_shape(road_releases):
    """The record the Postgres loader builds with `jsonb_build_array`, as JSON text."""
    out = fixups.build_road_metadata(_road_gdf([3061525]), _td([], ROADS), 66)

    assert json.loads(out["metadata"].iloc[0]) == [
        {
            "table_column": "name",
            "source": "linz_aims",
            "source_key_name": "road_id",
            "source_key_value": 3061525,
            "source_table": "roads",
            "source_column": "name",
            "source_updated_at": STAMP_66,
            "imported_at": STAMP_66,
        }
    ]


def test_build_road_metadata_leaves_unlinked_roads_null(road_releases):
    """`rna_sufi` 0 is "no AIMS road" - 60,210 of release 66's 153,518 rows. A record for road_id 0
    would assert a link to an AIMS road that does not exist."""
    out = fixups.build_road_metadata(_road_gdf([3061525, 0, 1771150]), _td([], ROADS), 66)

    assert out["metadata"].isna().tolist() == [False, True, False]


def test_build_road_metadata_stamps_the_release_not_the_wall_clock(road_releases):
    """Both timestamps are the release date. A build-time stamp would change the bytes of every
    record on every rebuild, so kart would see all ~93k features as modified each run."""
    out = fixups.build_road_metadata(_road_gdf([3061525]), _td([], ROADS), 66)

    record = json.loads(out["metadata"].iloc[0])[0]
    assert record["source_updated_at"] == record["imported_at"] == STAMP_66


def test_build_road_metadata_is_byte_stable(road_releases):
    """Same input -> same bytes, keys sorted and separators fixed: dict ordering drift would read
    as a changed feature downstream."""
    gdf = _road_gdf([3061525, 3061525])
    first = fixups.build_road_metadata(gdf, _td([], ROADS), 66)["metadata"]
    second = fixups.build_road_metadata(gdf, _td([], ROADS), 66)["metadata"]

    assert first.iloc[0] == first.iloc[1] == second.iloc[0]  # repeated sufi and repeated run agree
    assert first.iloc[0].startswith('[{"imported_at":')  # sorted keys, no whitespace


def test_build_road_metadata_does_not_mutate_its_input(road_releases):
    """`transform` reassigns the returned frame, but a fixup that edited in place would also have
    rewritten the caller's copy - and the raw sufi values are unrecoverable once overwritten."""
    gdf = _road_gdf([3061525])
    fixups.build_road_metadata(gdf, _td([], ROADS), 66)

    assert gdf["metadata"].tolist() == [3061525]


def test_build_source_metadata_without_a_sentinel_keys_every_non_null(road_releases):
    """`unset_key=None` - the shape a source with no "no link" sentinel needs, so only a NULL key
    means unlinked. Exercised through the shared helper because road's sufi always has one."""
    gdf = _road_gdf([0, 7, None])
    out = fixups._build_source_metadata(gdf, _td([], ROADS), 66, ROADS, fixups.ROAD_NAME_FROM_AIMS)

    assert out["metadata"].isna().tolist() == [False, False, True]  # 0 is a real key here
    assert json.loads(out["metadata"].iloc[0])[0]["source_key_value"] == 0


def test_build_source_metadata_writes_the_source_ref_as_json_keys(road_releases):
    """A `SourceRef`'s field names are the record's JSON keys, so a future `build_water_metadata`
    only has to declare its own ref rather than restate the record shape."""
    ref = fixups.SourceRef(
        table_column="name",
        source="nzgb_gazetteer",
        source_key_name="feat_id",
        source_table="nzgb_gaz",
        source_column="name",
    )
    out = fixups._build_source_metadata(_road_gdf([12345]), _td([], ROADS), 66, ROADS, ref)

    assert json.loads(out["metadata"].iloc[0]) == [
        {
            "table_column": "name",
            "source": "nzgb_gazetteer",
            "source_key_name": "feat_id",
            "source_key_value": 12345,
            "source_table": "nzgb_gaz",
            "source_column": "name",
            "source_updated_at": STAMP_66,
            "imported_at": STAMP_66,
        }
    ]


def test_build_road_metadata_rejects_wrong_dataset(road_releases):
    """Miswired to another dataset this would silently overwrite that dataset's `metadata`."""
    with pytest.raises(ValueError, match=f"fixup for dataset '{ROADS}' applied to 'other_dataset'"):
        fixups.build_road_metadata(_road_gdf([3061525]), _td([], "other_dataset"), 66)


def test_build_road_metadata_rejects_a_non_sufi_metadata_column(road_releases):
    """`metadata` not holding numbers means the config mapped something other than `$rna_sufi`
    into it; coercing would quietly yield an all-null column instead."""
    gdf = _road_gdf([3061525])
    gdf["metadata"] = ["not a sufi"]

    with pytest.raises((ValueError, TypeError)):
        fixups.build_road_metadata(gdf, _td([], ROADS), 66)


def test_build_road_metadata_rejects_an_unknown_release(road_releases):
    """No release date means no stamp; falling back to `now()` is exactly what this avoids."""
    with pytest.raises(LookupError, match="release 99 is not in the release config"):
        fixups.build_road_metadata(_road_gdf([3061525]), _td([], ROADS), 99)


def _status_of_a(gdf: gpd.GeoDataFrame) -> str:
    return gdf.loc[gdf["name"] == "a", "status"].iloc[0]


def _setup_shared_source(tmp_path, monkeypatch, *, fixups_cfg: list[dict]):
    """Two releases (60, 66) backed by one shared source file (mimicking the
    export-stage symlink dedup), a matching lifecycle, and a 'flag_a' fixup."""

    def flag_a(gdf, td, release_id):
        gdf.loc[gdf["name"] == "a", "status"] = "FIXED"
        return gdf

    monkeypatch.setitem(fixups.FIXUPS, "flag_a", flag_a)

    td = ThemeDataset.model_validate(
        {
            "name": "ds",
            "source": "kart@data.koordinates.com:linz/x-topo-150k",
            "mapping": {"name": "$", "status": "$"},
            "fixups": fixups_cfg,
        }
    )
    theme = Theme(name="t", target_repo="r", target_epsg="EPSG:4326", datasets=[td])
    releases = [Release(id=60, date=datetime(2020, 1, 1)), Release(id=66, date=datetime(2021, 1, 1))]

    monkeypatch.setattr(transform, "get_themes", lambda: [theme])
    monkeypatch.setattr(transform, "get_releases", lambda: releases)
    monkeypatch.setattr(transform, "WORKING_EXPORTS_DIR", tmp_path / "export")
    monkeypatch.setattr(transform, "WORKING_TRANSFORM_DIR", tmp_path / "transform")
    monkeypatch.setattr(fid_lifecycle, "WORKING_LIFECYCLE_DIR", tmp_path / "lifecycle")

    src = gpd.GeoDataFrame(
        {"t50_fid": [1, 2], "name": ["a", "b"], "status": ["live", "live"]},
        geometry=[Point(0, 0), Point(1, 1)],
        crs="EPSG:4326",
    )
    exp60 = tmp_path / "export" / "release_60"
    exp60.mkdir(parents=True)
    src_file = exp60 / "ds.json"
    src.to_file(src_file, driver="GeoJSON")
    exp66 = tmp_path / "export" / "release_66"
    exp66.mkdir(parents=True)
    (exp66 / "ds.json").symlink_to(src_file)  # shared source commit

    lifecycle_dir = tmp_path / "lifecycle"
    lifecycle_dir.mkdir()
    (lifecycle_dir / "ds_release60-66.json").write_text(
        json.dumps(
            {
                "1": {"id": "id-1", "created_at": "2020-01-01T00:00:00+00:00"},
                "2": {"id": "id-2", "created_at": "2020-01-01T00:00:00+00:00"},
            }
        )
    )


def test_fixup_on_non_canonical_release_is_rejected(tmp_path, monkeypatch):
    """Releases 60 and 66 share one source file; 60 is canonical (transformed),
    66 just symlinks to it. A fixup gated to 66 could never run -> config error."""
    _setup_shared_source(tmp_path, monkeypatch, fixups_cfg=[{"fn": "flag_a", "releases": [66]}])

    with pytest.raises(Exception, match="gate the fixup to the canonical release 60"):
        transform.transform_dataset_release("ds", 66)


def test_fixup_on_canonical_release_is_inherited_by_group(tmp_path, monkeypatch):
    """A fixup gated to the canonical release applies during its transform and is
    inherited by the source-sharing release via symlink (dedup preserved)."""
    _setup_shared_source(tmp_path, monkeypatch, fixups_cfg=[{"fn": "flag_a", "releases": [60]}])

    out66 = transform.transform_dataset_release("ds", 66)
    assert os.path.islink(out66)  # still deduped
    assert _status_of_a(transform.read_transform(out66)) == "FIXED"  # inherits the canonical's fix


def test_non_fixup_dataset_still_reuses_shared_source(tmp_path, monkeypatch):
    """Without fixups, a release sharing a source reuses the canonical transform
    via symlink (the dedup optimization is preserved)."""
    _setup_shared_source(tmp_path, monkeypatch, fixups_cfg=[])

    out66 = transform.transform_dataset_release("ds", 66)
    assert os.path.islink(out66)


def _contour_number_gdf() -> gpd.GeoDataFrame:
    """Two labels on the same contour direction, with downhill on opposite sides, plus one
    whose nearest-contour join found nothing so `label` is null."""
    return gpd.GeoDataFrame(
        {
            "orientation": [np.radians(30), np.radians(30), np.radians(30)],  # radians CCW from east
            "cont_rota": [150.0, 330.0, 150.0],  # degrees CW from north, downhill, either side
            "label": [100.0, 250.0, np.nan],
        },
        geometry=[Point(1600000, 5400000)] * 3,
        crs="EPSG:2193",
    )


def test_contour_number_orients_the_label_and_renders_the_elevation():
    """The tangent is radians counter-clockwise from east and the renderer wants degrees
    clockwise from east, so an unflipped label comes out as `-degrees(orientation)`. Where
    downhill sits on the other side of the contour the label is turned 180 degrees to keep
    the digits facing uphill."""
    out = fixups.contour_number(_contour_number_gdf(), _td([], name="contour_number"), 66)

    # 30 degrees CCW from east -> -30 -> 330 for the label that needs no flip
    assert out["orientation"].tolist() == [330, 150, 330]
    assert out["label"].tolist()[:2] == ["100", "250"]
