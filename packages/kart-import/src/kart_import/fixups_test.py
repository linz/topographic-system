import json
import os
from datetime import datetime

import geopandas as gpd
import pytest
from shapely.geometry import Point

from . import fixups
from .assets import fid_lifecycle, transform
from .assets.transform import apply_fixups
from .config import Release, Theme, ThemeDataset


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


def test_match_fids_handles_int_and_float_dtypes():
    """pyogrio may read an integer t50_fid as float; matching must work for both."""
    rows = [{"fid": 10}, {"fid": 20}, {"fid": 30}]
    for cast in (int, float):
        gdf = gpd.GeoDataFrame(
            {"fid": [cast(r["fid"]) for r in rows]},
            geometry=[Point(0, 0)] * len(rows),
            crs="EPSG:4326",
        )
        assert fixups._match_fids(gdf, {10, 30}).tolist() == [True, False, True]


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
