import json
from datetime import datetime
from pathlib import Path

import geopandas as gpd
from shapely.geometry import LineString, Point

from .. import export_paths
from ..config import Release, Source, ThemeDataset
from . import fid_lifecycle
from .extract.export import write_export


def test_update_lifecycle_tracks_additions_modifications_and_unchanged():
    """New features get created_at = updated_at; unchanged features keep updated_at;
    modified features bump updated_at while preserving id and created_at."""
    lifecycle: dict = {}
    t1 = "2024-01-01T00:00:00+00:00"
    t2 = "2025-01-01T00:00:00+00:00"

    gdf1 = gpd.GeoDataFrame(
        {"t50_fid": [1, 2], "name": ["A", "B"]},
        geometry=[Point(0, 0), Point(1, 1)],
        crs="EPSG:4326",
    )
    digests = fid_lifecycle.update_lifecycle_from_frame(gdf1, lifecycle, {}, t1, "ds", "t50_fid")
    id_1 = lifecycle["1"]["id"]

    # fid 1 unchanged, fid 2 renamed, fid 3 added
    gdf2 = gpd.GeoDataFrame(
        {"t50_fid": [1, 2, 3], "name": ["A", "B-edited", "C"]},
        geometry=[Point(0, 0), Point(1, 1), Point(2, 2)],
        crs="EPSG:4326",
    )
    fid_lifecycle.update_lifecycle_from_frame(gdf2, lifecycle, digests, t2, "ds", "t50_fid")

    assert lifecycle["1"] == {"id": id_1, "created_at": t1, "updated_at": t1}
    assert lifecycle["2"]["created_at"] == t1
    assert lifecycle["2"]["updated_at"] == t2
    assert lifecycle["2"]["updates"] == [t2]
    assert lifecycle["3"]["created_at"] == t2
    assert lifecycle["3"]["updated_at"] == t2
    assert "updates" not in lifecycle["3"]


def test_updated_at_for_release_does_not_leak_future_updates():
    """Each release sees the latest modification timestamp <= its cutoff, never a future one."""
    t1 = "2015-12-21T20:14:38Z"
    t2 = "2020-06-10T05:00:00Z"
    t3 = "2024-03-01T00:00:00Z"
    entry = {"id": "u1", "created_at": t1, "updated_at": t3, "updates": [t2, t3]}

    assert fid_lifecycle.updated_at_for_release(entry, datetime(2016, 1, 1)) == t1
    assert fid_lifecycle.updated_at_for_release(entry, datetime(2020, 6, 10, 5, 0, 0)) == t2
    assert fid_lifecycle.updated_at_for_release(entry, datetime(2023, 1, 1)) == t2
    assert fid_lifecycle.updated_at_for_release(entry, datetime(2025, 1, 1)) == t3
    assert fid_lifecycle.updated_at_for_release(entry, None) == t3


def test_update_lifecycle_ignores_sub_millimetre_geometry_jitter():
    """Sub-millimetre float noise in projected (1e-3 m) or geographic (1e-8 deg) exports
    should not bump updated_at, while real geometry edits still do."""
    lifecycle: dict = {}
    t1 = "2015-11-19T02:33:38Z"
    t2 = "2020-11-17T02:42:53Z"
    t3 = "2021-02-19T07:00:51Z"

    # Projected CRS (EPSG:2193): 5e-10 m jitter should be ignored, 1 m move should register
    gdf1 = gpd.GeoDataFrame(
        {"t50_fid": [2111334]},
        geometry=[LineString([(1226204.0569910007, 4883179.698231999), (1226139.8711170005, 4883177.46908)])],
        crs="EPSG:2193",
    )
    gdf2 = gpd.GeoDataFrame(
        {"t50_fid": [2111334]},
        geometry=[LineString([(1226204.0569910002, 4883179.698231999), (1226139.8711170002, 4883177.469079999)])],
        crs="EPSG:2193",
    )
    gdf3 = gpd.GeoDataFrame(
        {"t50_fid": [2111334]},
        geometry=[LineString([(1226205.0569910002, 4883179.698231999), (1226139.8711170002, 4883177.469079999)])],
        crs="EPSG:2193",
    )

    digests = fid_lifecycle.update_lifecycle_from_frame(gdf1, lifecycle, {}, t1, "ds", "t50_fid")
    digests = fid_lifecycle.update_lifecycle_from_frame(gdf2, lifecycle, digests, t2, "ds", "t50_fid")
    assert lifecycle["2111334"]["updated_at"] == t1
    assert "updates" not in lifecycle["2111334"]

    fid_lifecycle.update_lifecycle_from_frame(gdf3, lifecycle, digests, t3, "ds", "t50_fid")
    assert lifecycle["2111334"]["updated_at"] == t3
    assert lifecycle["2111334"]["updates"] == [t3]


def test_update_lifecycle_reads_auto_pk_renamed_to_fid():
    """topographic-source-data renamed auto_pk to fid in its release 67 reload: rows before and after
    the rename are both keyed."""
    lifecycle: dict = {}
    gdf1 = gpd.GeoDataFrame({"auto_pk": [128506]}, geometry=[Point(0, 0)], crs="EPSG:4326")
    gdf2 = gpd.GeoDataFrame({"fid": [1]}, geometry=[Point(1, 1)], crs="EPSG:4326")

    digests = fid_lifecycle.update_lifecycle_from_frame(
        gdf1, lifecycle, {}, "2026-09-14T00:00:00+00:00", "ds", "auto_pk"
    )
    fid_lifecycle.update_lifecycle_from_frame(gdf2, lifecycle, digests, "2026-10-19T00:00:00+00:00", "ds", "auto_pk")

    assert {fid: entry["created_at"] for fid, entry in lifecycle.items()} == {
        "128506": "2026-09-14T00:00:00+00:00",
        "1": "2026-10-19T00:00:00+00:00",
    }


def test_generate_lifecycle_from_exported_parquets_without_clone(tmp_path, monkeypatch):
    """End to end: `generate_lifecycle` derives IDs/timestamps purely from `data/export/`
    (parquets + schema sidecars + `releases.json`), with no source clone present."""
    td = ThemeDataset(
        name="ds",
        source=Source(url="kart@data.koordinates.com:linz/x-topo-150k"),
    )
    releases = [Release(id=66, date=datetime(2025, 1, 1)), Release(id=67, date=datetime(2026, 1, 1))]
    monkeypatch.setattr(export_paths, "EXPORT_DIR", tmp_path / "export")
    monkeypatch.setattr(export_paths, "get_source_entry", lambda _: td)
    monkeypatch.setattr(fid_lifecycle, "get_dataset_by_name", lambda _: td)
    monkeypatch.setattr(fid_lifecycle, "get_releases", lambda: releases)
    monkeypatch.setattr(fid_lifecycle, "WORKING_LIFECYCLE_DIR", tmp_path / "lifecycle")

    entry_dir = export_paths.export_entry_dir("ds")
    entry_dir.mkdir(parents=True)

    f1 = export_paths.entry_commit_file("ds", "aaa")
    f2 = export_paths.entry_commit_file("ds", "bbb")
    write_export(
        gpd.GeoDataFrame({"t50_fid": [10.0], "name": ["old"]}, geometry=[Point(0, 0)], crs="EPSG:4326"),
        f1,
    )
    write_export(
        gpd.GeoDataFrame(
            {"t50_fid": [10.0, 20.0], "name": ["new", "added"]}, geometry=[Point(0, 0), Point(1, 1)], crs="EPSG:4326"
        ),
        f2,
    )
    schema = json.dumps([{"name": "t50_fid", "dataType": "integer"}])
    export_paths.schema_sidecar_file(f1).write_text(schema)
    export_paths.schema_sidecar_file(f2).write_text(schema)

    export_paths.write_releases_map(
        entry_dir,
        "ds",
        {
            66: {"commit": "aaa", "commit_time": "2025-01-01T00:00:00+00:00", "location": f1.name},
            67: {"commit": "bbb", "commit_time": "2026-01-01T00:00:00+00:00", "location": f2.name},
        },
        dataset_id="x-topo-150k",
    )

    out_path = fid_lifecycle.generate_lifecycle("ds")
    data = json.loads(Path(out_path).read_text())

    # Schema sidecar restored t50_fid from float64 to integer, so keys are "10" and "20", not "10.0".
    assert set(data) == {"10", "20"}
    assert data["10"]["created_at"] == "2025-01-01T00:00:00+00:00"
    assert data["10"]["updated_at"] == "2026-01-01T00:00:00+00:00"
    assert data["20"]["created_at"] == "2026-01-01T00:00:00+00:00"
    assert data["20"]["updated_at"] == "2026-01-01T00:00:00+00:00"
