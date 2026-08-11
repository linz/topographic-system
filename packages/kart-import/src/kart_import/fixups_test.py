import json
import os
from datetime import datetime

import geopandas as gpd
import numpy as np
import pytest
from shapely.geometry import LineString, Point
from shapely.geometry.base import BaseGeometry
from shapely.wkt import loads

from . import fixups
from .assets import fid_lifecycle, transform
from .assets.transform import apply_fixups
from .config import Release, Theme, ThemeDataset
from .fixups import drop_degenerate_fences


def _gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame({"name": ["Broken", "OK"]}, geometry=[Point(0, 0), Point(1, 1)], crs="EPSG:4326")


def _td(fixups_cfg: list[dict]) -> ThemeDataset:
    return ThemeDataset.model_validate(
        {"name": "t", "source": "kart@data.koordinates.com:linz/x-topo-150k", "fixups": fixups_cfg}
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


EMPTY = loads("LINESTRING EMPTY")
REAL = LineString([(174.0, -41.0), (174.1, -41.1)])
DEGENERATE_FIDS = [7640059, 7640098, 7704786, 7704787]


def _fence_gdf(rows: list[tuple[float, BaseGeometry | None]]) -> gpd.GeoDataFrame:
    """A nz_fence_centrelines-shaped frame from (t50_fid, geometry) rows.

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
        ([(7640059, EMPTY), (123, REAL)], [123]),
        # The gate: a fid repaired upstream is kept rather than dropped by a stale list.
        ([(7640059, REAL), (123, REAL)], [7640059, 123]),
        # Deliberately narrow: an unexpected empty geometry is left to fail loudly at the
        # FlatGeobuf write rather than being silently swallowed here.
        ([(999, EMPTY), (123, REAL)], [999, 123]),
        ([(7704786, None), (123, REAL)], [123]),
        ([(123, REAL), (456, REAL)], [123, 456]),
        ([(fid, EMPTY) for fid in DEGENERATE_FIDS] + [(123, REAL)], [123]),
    ],
    ids=[
        "listed-and-collapsed-dropped",
        "listed-but-repaired-kept",
        "unlisted-empty-kept",
        "listed-with-null-geometry-dropped",
        "nothing-listed-is-a-noop",
        "all-four-dropped",
    ],
)
def test_drop_degenerate_fences(rows, survivors):
    out = drop_degenerate_fences(_fence_gdf(rows), _td([]), 60)
    assert out["t50_fid"].tolist() == survivors
    # theme.py concatenates with ignore_index, but a gappy index would still surface in any
    # positional lookup downstream.
    assert out.index.tolist() == list(range(len(survivors)))


@pytest.mark.parametrize("cast", [int, float], ids=["int", "float"])
def test_drop_degenerate_fences_matches_either_fid_dtype(cast):
    """pyogrio may read an integer t50_fid as float; matching must work for both."""
    out = drop_degenerate_fences(_fence_gdf([(cast(7640059), EMPTY), (cast(123), REAL)]), _td([]), 60)
    assert out["t50_fid"].tolist() == [cast(123)]


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
