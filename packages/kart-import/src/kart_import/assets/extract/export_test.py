import json
from decimal import Decimal

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pytest
from shapely.geometry import Point

from ... import export_paths
from ...config import Source
from ...export_paths import export_filename
from . import export

REPO_URL = "git@github.com:linz/topographic-source-data"


def _fake_entry(dataset_name: str, **kwargs):
    from types import SimpleNamespace

    # dataset=None (a single-dataset Koordinates repo) makes get_source_repo_name return the
    # entry's own name, so export_entry_dir(name) == EXPORT_DIR / name -- the simplest case.
    return SimpleNamespace(name=dataset_name, source=Source(url=REPO_URL, dataset=None), **kwargs)


def _patch_source_entry(monkeypatch, entry_fn):
    """`export.py` and `export_paths.py` each import `get_source_entry` independently; keep them
    in sync so export_paths' EXPORT_DIR-based paths resolve the same repo as export.py's own
    clone-path computation (`get_source_repo_name` itself is pure, so it needs no patch)."""
    monkeypatch.setattr(export, "get_source_entry", entry_fn)
    monkeypatch.setattr(export_paths, "get_source_entry", entry_fn)


def _patch_simple_export_layout(monkeypatch, tmp_path):
    """For tests using a synthetic dataset name not registered in the real config: make
    `export_entry_dir("ds")` resolve to `tmp_path / "ds"`, the same shape EXPORT_DIR-based paths
    always have, without needing a real DATASET_MAP entry."""
    monkeypatch.setattr(export_paths, "EXPORT_DIR", tmp_path)
    _patch_source_entry(monkeypatch, _fake_entry)


def test_export_empty_releases_shares_one_file(tmp_path, monkeypatch):
    """The `export` rule declares an output per release, so a dataset whose repo starts partway
    through the series needs a file for the releases predating it. One shared file, symlinked like
    the commit exports, so the transform fingerprint normalises them once."""
    monkeypatch.setattr(export_paths, "WORKING_EXPORTS_DIR", tmp_path)
    _patch_simple_export_layout(monkeypatch, tmp_path)

    export.export_empty_releases("ds", [30, 31, 32])

    links = [tmp_path / f"release_{r}" / export_filename("ds") for r in (30, 31, 32)]
    assert all(link.is_symlink() for link in links)
    assert len({link.resolve() for link in links}) == 1  # one file behind all three


def test_export_empty_releases_is_readable_as_an_empty_frame(tmp_path, monkeypatch):
    """What transform reads back, and what makes it emit an empty transform rather than pushing
    a featureless frame through the column-wise normalisers."""
    monkeypatch.setattr(export_paths, "WORKING_EXPORTS_DIR", tmp_path)
    _patch_simple_export_layout(monkeypatch, tmp_path)

    export.export_empty_releases("ds", [30])

    gdf = export.gpd_read(tmp_path / "release_30" / export_filename("ds"))
    assert gdf.empty


def test_export_empty_releases_with_no_releases_writes_nothing(tmp_path, monkeypatch):
    """The normal case: a dataset covering every release must not gain a stray empty export."""
    monkeypatch.setattr(export_paths, "WORKING_EXPORTS_DIR", tmp_path)

    export.export_empty_releases("ds", [])

    assert list(tmp_path.iterdir()) == []


def test_link_release_export_replaces_a_dangling_link(tmp_path, monkeypatch):
    """Snakemake deletes the outputs of a failed job, so a rerun can find a link whose target is
    gone. Such a link reads as non-existent, and `symlink` onto it would fail with EEXIST."""
    monkeypatch.setattr(export_paths, "WORKING_EXPORTS_DIR", tmp_path)
    target = tmp_path / "ds" / export_filename("commit")
    target.parent.mkdir()
    target.write_text("{}")

    export.link_release_export(target, "ds", 30)
    target.unlink()  # dangling
    target.write_text("{}")
    export.link_release_export(target, "ds", 30)

    assert (tmp_path / "release_30" / export_filename("ds")).resolve() == target


def test_releases_before_the_dataset_exists_export_empty(tmp_path, monkeypatch):
    """A dataset added partway through a shared source repo's history (`site_bdy` in
    topographic-source-data) isn't in the commits earlier releases resolve to, so those releases get
    the empty export rather than a failing `kart export`."""
    from types import SimpleNamespace

    monkeypatch.setattr(export_paths, "WORKING_EXPORTS_DIR", tmp_path)
    monkeypatch.setattr(export_paths, "EXPORT_DIR", tmp_path)
    monkeypatch.setattr(export, "SOURCE_DIR", tmp_path / "source")
    monkeypatch.setattr(export, "is_kart", lambda _: True)
    _patch_source_entry(
        monkeypatch, lambda _: SimpleNamespace(name="ds", source=Source(url=REPO_URL, dataset="ds"))
    )
    monkeypatch.setattr(export, "get_releases", lambda: [SimpleNamespace(id=r, until=None) for r in (66, 67)])
    monkeypatch.setattr(export, "get_release_commit", lambda _, until: ("before", "2026-09-14"))
    monkeypatch.setattr(export, "ref_has_dataset", lambda *_: False)
    monkeypatch.setattr(export, "run_command", lambda *_, **__: pytest.fail("must not kart export"))

    export.export_dataset_releases("ds")

    empty_name = export_paths.dataset_empty_file("ds").name
    assert (tmp_path / "release_66" / export_filename("ds")).resolve().name == empty_name
    assert (tmp_path / "release_67" / export_filename("ds")).resolve().name == empty_name


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
    """No commit to backfill from: the releases still need `export_empty_releases`."""
    assert export.backfill_releases({}, [30, 31]) == [30, 31]


def test_release_commit_map_flattens_by_release(monkeypatch):
    """One entry per release id, `None` for a release predating the dataset's first commit."""
    monkeypatch.setattr(export, "resolve_release_commits", lambda name: (_commits(), [30, 31]))

    assert export.release_commit_map("ds") == {30: None, 31: None, 66: "a", 67: "b"}


def test_backfill_dataset_links_predating_releases_to_its_earliest_export(tmp_path, monkeypatch):
    """End to end through `export_dataset_releases`: release 30 predates the source and links to
    the same commit export as release 66 instead of the empty layer."""
    from types import SimpleNamespace

    monkeypatch.setattr(export_paths, "WORKING_EXPORTS_DIR", tmp_path)
    monkeypatch.setattr(export_paths, "EXPORT_DIR", tmp_path)
    monkeypatch.setattr(export, "SOURCE_DIR", tmp_path / "source")
    monkeypatch.setattr(export, "is_kart", lambda _: True)
    _patch_source_entry(
        monkeypatch,
        lambda _: SimpleNamespace(name="ds", source=Source(url=REPO_URL, dataset="ds"), backfill=True),
    )
    monkeypatch.setattr(export, "get_releases", lambda: [SimpleNamespace(id=r, until=r) for r in (30, 66)])
    monkeypatch.setattr(
        export, "get_release_commit", lambda _, until: None if until == 30 else ("abc", "2026-05-15T12:27:14+12:00")
    )
    monkeypatch.setattr(export, "ref_has_dataset", lambda *_: True)
    monkeypatch.setattr(export, "run_command", lambda cmd, **_: open(cmd[-1], "w").close())
    monkeypatch.setattr(export, "get_dataset_schema", lambda *_: ())

    export.export_dataset_releases("ds")

    linked = {r: (tmp_path / f"release_{r}" / export_filename("ds")).resolve() for r in (30, 66)}
    assert linked[30] == linked[66]
    assert linked[30].name != export_filename("empty")


def test_export_dataset_merges_lifecycle_and_export(monkeypatch):
    """`export_dataset` is the single entry point for the two clone-only, stage-1-only steps that
    have no dependency on each other's output -- merged so there's one rule/CLI invocation."""
    calls = []
    monkeypatch.setattr(export, "generate_lifecycle", lambda name: calls.append(("lifecycle", name)))
    monkeypatch.setattr(export, "export_dataset_releases", lambda name: calls.append(("export", name)) or "result")

    result = export.export_dataset("ds")

    assert calls == [("lifecycle", "ds"), ("export", "ds")]
    assert result == "result"


def test_export_dataset_releases_writes_schema_sidecar_and_releases_map(tmp_path, monkeypatch):
    """The per-commit schema sidecar and the dataset's release map -- what lets `transform`/
    `prepare_lookup` and `relink_dataset_releases` work with no clone present."""
    from types import SimpleNamespace

    monkeypatch.setattr(export_paths, "WORKING_EXPORTS_DIR", tmp_path)
    _patch_simple_export_layout(monkeypatch, tmp_path)
    monkeypatch.setattr(export, "SOURCE_DIR", tmp_path / "source")
    monkeypatch.setattr(export, "is_kart", lambda _: True)
    monkeypatch.setattr(export, "get_releases", lambda: [SimpleNamespace(id=r, until=r) for r in (66, 67)])
    monkeypatch.setattr(export, "get_release_commit", lambda _, until: ("abc", "2026-05-15T12:27:14+12:00"))
    monkeypatch.setattr(export, "ref_has_dataset", lambda *_: True)
    monkeypatch.setattr(export, "run_command", lambda cmd, **_: open(cmd[-1], "w").close())
    monkeypatch.setattr(export, "get_dataset_schema", lambda *_: ({"name": "t50_fid", "dataType": "integer"},))
    monkeypatch.setattr(export, "get_kart_dataset_id", lambda _: "ds")  # source.dataset is None here

    export.export_dataset_releases("ds")

    commit_file = export_paths.dataset_commit_file("ds", "abc")
    assert commit_file.exists()
    assert json.loads(export_paths.schema_sidecar_file(commit_file).read_text()) == [
        {"name": "t50_fid", "dataType": "integer"}
    ]

    location = commit_file.name
    assert export_paths.load_releases_map(export_paths.export_entry_dir("ds"), "ds") == {
        66: {"commit": "abc", "location": location},
        67: {"commit": "abc", "location": location},
    }


def test_relink_dataset_releases_rebuilds_symlinks_from_the_map(tmp_path, monkeypatch):
    """The inverse of the write above -- what `assets.working_data`'s download runs after
    pulling per-commit exports from S3, with no clone involved."""
    monkeypatch.setattr(export_paths, "WORKING_EXPORTS_DIR", tmp_path)
    _patch_simple_export_layout(monkeypatch, tmp_path)
    commit_file = export_paths.dataset_commit_file("ds", "abc")
    commit_file.parent.mkdir(parents=True)
    commit_file.write_text("data")
    export_paths.write_releases_map(
        export_paths.export_entry_dir("ds"),
        "ds",
        {66: {"commit": "abc", "location": commit_file.name}},
    )

    export.relink_dataset_releases("ds")

    link = tmp_path / "release_66" / export_filename("ds")
    assert link.is_symlink()
    assert link.resolve() == commit_file


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
