import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pytest
from shapely.geometry import Point

from ... import export_paths
from ...config import Source
from . import export

REPO_URL = "git@github.com:linz/topographic-source-data"


def _fake_entry(entry_name: str, **kwargs):
    # dataset=None (a single-dataset Koordinates repo) makes get_source_repo_name return the
    # entry's own name, so export_entry_dir(name) == EXPORT_DIR / name -- the simplest case.
    return SimpleNamespace(name=entry_name, source=Source(url=REPO_URL, dataset=None), **kwargs)


def _patch_source_entry(monkeypatch, entry_fn):
    """`export.py` and `export_paths.py` each import `get_source_entry` independently; keep them
    in sync so export_paths' EXPORT_DIR-based paths resolve the same repo as export.py's own
    clone-path computation (`get_source_repo_name` itself is pure, so it needs no patch)."""
    monkeypatch.setattr(export, "get_source_entry", entry_fn)
    monkeypatch.setattr(export_paths, "get_source_entry", entry_fn)


def _patch_simple_export_layout(monkeypatch, tmp_path):
    """For tests using a synthetic entry name not registered in the real config: make
    `export_entry_dir("ds")` resolve to `tmp_path / "ds"`, the same shape EXPORT_DIR-based paths
    always have, without needing a real DATASET_MAP entry."""
    monkeypatch.setattr(export_paths, "EXPORT_DIR", tmp_path)
    _patch_source_entry(monkeypatch, _fake_entry)


def _commits() -> dict[str, export.CommitData]:
    return {
        "b": export.CommitData(commit="b", commit_time="2026-09-14T11:28:50+12:00", releases=[67]),
        "a": export.CommitData(commit="a", commit_time="2026-05-15T12:27:14+12:00", releases=[66]),
    }


def test_backfill_releases_go_to_the_earliest_commit():
    """Releases predating the source share its earliest export, ahead of the release it already had."""
    commits = _commits()

    assert export.backfill_releases(commits, [30, 31]) == []
    assert commits["a"].releases == [30, 31, 66]
    assert commits["b"].releases == [67]


def test_backfill_releases_without_any_commit_leaves_them_empty():
    """No commit to backfill from: the releases stay predating (recorded as null in releases.json)."""
    assert export.backfill_releases({}, [30, 31]) == [30, 31]


def test_releases_before_the_dataset_exists_record_null_location(tmp_path, monkeypatch):
    """A dataset added partway through a shared source repo's history (`site_bdy` in
    topographic-source-data) isn't in the commits earlier releases resolve to, so those releases
    record `commit: None, location: None` rather than running `kart export`."""
    monkeypatch.setattr(export_paths, "EXPORT_DIR", tmp_path)
    monkeypatch.setattr(export, "SOURCE_DIR", tmp_path / "source")
    monkeypatch.setattr(export, "is_kart", lambda _: True)
    _patch_source_entry(monkeypatch, lambda _: SimpleNamespace(name="ds", source=Source(url=REPO_URL, dataset="ds")))
    monkeypatch.setattr(export, "get_releases", lambda: [SimpleNamespace(id=r, until=None) for r in (66, 67)])
    monkeypatch.setattr(export, "get_release_commit", lambda _, until: ("before", "2026-09-14"))
    monkeypatch.setattr(export, "ref_has_dataset", lambda *_: False)
    monkeypatch.setattr(export, "run_command", lambda *_, **__: pytest.fail("must not kart export"))

    export.export_entry("ds")

    assert export_paths.load_releases_map(export_paths.export_entry_dir("ds"), "ds") == {
        66: {"commit": None, "commit_time": None, "location": None},
        67: {"commit": None, "commit_time": None, "location": None},
    }
    assert export_paths.resolve_release_export("ds", 66) is None
    assert export_paths.entry_exported_sentinel("ds").exists()


def test_backfill_dataset_maps_predating_releases_to_earliest_export(tmp_path, monkeypatch):
    """End to end through `export_entry`: release 30 predates the source, resolves to the same
    commit export as release 66, and is flagged `backfilled: True` in `releases.json`."""
    monkeypatch.setattr(export_paths, "EXPORT_DIR", tmp_path)
    monkeypatch.setattr(export, "SOURCE_DIR", tmp_path / "source")
    monkeypatch.setattr(export, "is_kart", lambda _: True)
    entry = SimpleNamespace(name="ds", source=Source(url=REPO_URL, dataset="ds"), backfill=True)
    _patch_source_entry(monkeypatch, lambda _: entry)
    monkeypatch.setattr(export, "get_dataset_by_name", lambda _: entry)
    monkeypatch.setattr(export, "get_releases", lambda: [SimpleNamespace(id=r, until=r) for r in (30, 66)])
    monkeypatch.setattr(
        export, "get_release_commit", lambda _, until: None if until == 30 else ("abc", "2026-05-15T12:27:14+12:00")
    )
    monkeypatch.setattr(export, "ref_has_dataset", lambda *_: True)
    monkeypatch.setattr(export, "run_command", lambda cmd, **_: open(cmd[-1], "w").close())
    monkeypatch.setattr(export, "get_dataset_schema", lambda *_: ())

    export.export_entry("ds")

    assert export_paths.resolve_release_export("ds", 30) == export_paths.resolve_release_export("ds", 66)
    assert export.backfilled_release_ids("ds") == {30}


def test_export_entry_writes_schema_sidecar_releases_map_and_sentinel(tmp_path, monkeypatch):
    """Per-commit parquet, schema sidecar, release map (with commit_time + dataset_id) and sentinel
    -- everything Stage 2 (`fid_lifecycle`, `prepare_lookup`, `transform`) needs with no clone."""
    _patch_simple_export_layout(monkeypatch, tmp_path)
    monkeypatch.setattr(export, "SOURCE_DIR", tmp_path / "source")
    monkeypatch.setattr(export, "is_kart", lambda _: True)
    monkeypatch.setattr(export, "get_releases", lambda: [SimpleNamespace(id=r, until=r) for r in (66, 67)])
    monkeypatch.setattr(export, "get_release_commit", lambda _, until: ("abc", "2026-05-15T12:27:14+12:00"))
    monkeypatch.setattr(export, "ref_has_dataset", lambda *_: True)
    cmds = []
    monkeypatch.setattr(export, "run_command", lambda cmd, **_: cmds.append(cmd) or open(cmd[-1], "w").close())
    monkeypatch.setattr(export, "get_dataset_schema", lambda *_: ({"name": "t50_fid", "dataType": "integer"},))
    monkeypatch.setattr(export, "get_kart_dataset_id", lambda _: "ds_kart_id")

    export.export_entry("ds")

    commit_file = export_paths.entry_commit_file("ds", "abc")
    tmp_commit_file = commit_file.with_name(f".tmp_{commit_file.name}")
    assert commit_file.exists()
    assert not tmp_commit_file.exists()
    if export.EXPORT_FORMAT == "parquet":
        assert cmds == [
            [
                "kart",
                "export",
                "--overwrite",
                "--ref",
                "abc",
                "-lco",
                "COMPRESSION=ZSTD",
                "-lco",
                "COMPRESSION_LEVEL=9",
                "ds_kart_id",
                str(tmp_commit_file),
            ]
        ]
    assert json.loads(export_paths.schema_sidecar_file(commit_file).read_text()) == [
        {"name": "t50_fid", "dataType": "integer"}
    ]

    meta = export_paths.load_releases_metadata(export_paths.export_entry_dir("ds"), "ds")
    assert meta["dataset_id"] == "ds_kart_id"
    assert export_paths.load_releases_map(export_paths.export_entry_dir("ds"), "ds") == {
        66: {"commit": "abc", "commit_time": "2026-05-15T12:27:14+12:00", "location": commit_file.name},
        67: {"commit": "abc", "commit_time": "2026-05-15T12:27:14+12:00", "location": commit_file.name},
    }
    assert export_paths.entry_exported_sentinel("ds").exists()


def test_export_entry_prunes_stale_commits_and_sidecars(tmp_path, monkeypatch):
    """Exports for commits no longer referenced by any release are pruned along with their sidecars,
    leaving sibling entries in the same repo dir untouched."""
    _patch_simple_export_layout(monkeypatch, tmp_path)
    monkeypatch.setattr(export, "SOURCE_DIR", tmp_path / "source")
    monkeypatch.setattr(export, "is_kart", lambda _: True)
    monkeypatch.setattr(export, "get_releases", lambda: [SimpleNamespace(id=66, until=66)])
    monkeypatch.setattr(export, "get_release_commit", lambda _, until: ("newsha", "2026-05-15T12:27:14+12:00"))
    monkeypatch.setattr(export, "ref_has_dataset", lambda *_: True)
    monkeypatch.setattr(export, "run_command", lambda cmd, **_: open(cmd[-1], "w").close())
    monkeypatch.setattr(export, "get_dataset_schema", lambda *_: ())
    monkeypatch.setattr(export, "get_kart_dataset_id", lambda _: "ds")

    entry_dir = export_paths.export_entry_dir("ds")
    entry_dir.mkdir(parents=True)
    stale = export_paths.entry_commit_file("ds", "oldsha")
    stale.write_text("old")
    stale_schema = export_paths.schema_sidecar_file(stale)
    stale_schema.write_text("[]")
    sibling = entry_dir / export_paths.entry_export_filename("other", "oldsha")
    sibling.write_text("keep")

    export.export_entry("ds")

    assert not stale.exists()
    assert not stale_schema.exists()
    assert sibling.exists()
    assert export_paths.entry_commit_file("ds", "newsha").exists()


def test_gpd_read_returns_parquet_decimals_as_floats(tmp_path):
    """A kart numeric exports to Parquet as a decimal; it reads back as float64, as it did from OGR."""
    path = tmp_path / "ds.parquet"
    gdf = gpd.GeoDataFrame(
        {"orientation": pd.array([Decimal("4.7069"), None], dtype=pd.ArrowDtype(pa.decimal128(10, 4)))},
        geometry=[Point(0, 0), Point(1, 1)],
        crs="EPSG:2193",
    )
    gdf.to_parquet(path)

    out = export.gpd_read(path)

    assert out["orientation"].dtype == "float64"
    assert out["orientation"].iloc[0] == 4.7069
    assert pd.isna(out["orientation"].iloc[1])


def test_export_entry_cleans_up_temp_file_on_kart_export_failure(tmp_path, monkeypatch):
    """A failed or interrupted `kart export` must not leave a partial parquet at the final path."""
    _patch_simple_export_layout(monkeypatch, tmp_path)
    monkeypatch.setattr(export, "SOURCE_DIR", tmp_path / "source")
    monkeypatch.setattr(export, "is_kart", lambda _: True)
    monkeypatch.setattr(export, "get_releases", lambda: [SimpleNamespace(id=66, until=66)])
    monkeypatch.setattr(export, "get_release_commit", lambda _, until: ("abc", "2026-05-15T12:27:14+12:00"))
    monkeypatch.setattr(export, "ref_has_dataset", lambda *_: True)
    monkeypatch.setattr(export, "get_kart_dataset_id", lambda _: "ds")

    def fail_mid_write(cmd, **_):
        Path(cmd[-1]).write_text("partial")
        raise RuntimeError("kart export failed")

    monkeypatch.setattr(export, "run_command", fail_mid_write)

    with pytest.raises(RuntimeError, match="kart export failed"):
        export.export_entry("ds")

    commit_file = export_paths.entry_commit_file("ds", "abc")
    assert not commit_file.exists()
    assert not commit_file.with_name(f".tmp_{commit_file.name}").exists()
