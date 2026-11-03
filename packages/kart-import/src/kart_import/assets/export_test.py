from decimal import Decimal

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pytest
from shapely.geometry import Point

from .. import export_paths
from ..config import Source
from ..export_paths import export_filename
from . import export

REPO_URL = "git@github.com:linz/topographic-source-data"


def test_export_empty_releases_shares_one_file(tmp_path, monkeypatch):
    """The `export` rule declares an output per release, so a dataset whose repo starts partway
    through the series needs a file for the releases predating it. One shared file, symlinked like
    the commit exports, so the transform fingerprint normalises them once."""
    monkeypatch.setattr(export_paths, "WORKING_EXPORTS_DIR", tmp_path)

    export.export_empty_releases("ds", [30, 31, 32])

    links = [tmp_path / f"release_{r}" / export_filename("ds") for r in (30, 31, 32)]
    assert all(link.is_symlink() for link in links)
    assert len({link.resolve() for link in links}) == 1  # one file behind all three


def test_export_empty_releases_is_readable_as_an_empty_frame(tmp_path, monkeypatch):
    """What transform reads back, and what makes it emit an empty transform rather than pushing
    a featureless frame through the column-wise normalisers."""
    monkeypatch.setattr(export_paths, "WORKING_EXPORTS_DIR", tmp_path)

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
    monkeypatch.setattr(export, "SOURCE_DIR", tmp_path / "source")
    monkeypatch.setattr(export, "is_kart", lambda _: True)
    monkeypatch.setattr(
        export, "get_source_entry", lambda _: SimpleNamespace(name="ds", source=Source(url=REPO_URL, dataset="ds"))
    )
    monkeypatch.setattr(export, "get_releases", lambda: [SimpleNamespace(id=r, until=None) for r in (66, 67)])
    monkeypatch.setattr(export, "get_release_commit", lambda _, until: ("before", "2026-09-14"))
    monkeypatch.setattr(export, "ref_has_dataset", lambda *_: False)
    monkeypatch.setattr(export, "run_command", lambda *_, **__: pytest.fail("must not kart export"))

    export.export_dataset_releases("ds")

    assert (tmp_path / "release_66" / export_filename("ds")).resolve().name == export_filename("empty")
    assert (tmp_path / "release_67" / export_filename("ds")).resolve().name == export_filename("empty")


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


def test_backfill_dataset_links_predating_releases_to_its_earliest_export(tmp_path, monkeypatch):
    """End to end through `export_dataset_releases`: release 30 predates the source and links to
    the same commit export as release 66 instead of the empty layer."""
    from types import SimpleNamespace

    monkeypatch.setattr(export_paths, "WORKING_EXPORTS_DIR", tmp_path)
    monkeypatch.setattr(export, "SOURCE_DIR", tmp_path / "source")
    monkeypatch.setattr(export, "is_kart", lambda _: True)
    monkeypatch.setattr(
        export,
        "get_source_entry",
        lambda _: SimpleNamespace(name="ds", source=Source(url=REPO_URL, dataset="ds"), backfill=True),
    )
    monkeypatch.setattr(export, "get_releases", lambda: [SimpleNamespace(id=r, until=r) for r in (30, 66)])
    monkeypatch.setattr(
        export, "get_release_commit", lambda _, until: None if until == 30 else ("abc", "2026-05-15T12:27:14+12:00")
    )
    monkeypatch.setattr(export, "ref_has_dataset", lambda *_: True)
    monkeypatch.setattr(export, "run_command", lambda cmd, **_: open(cmd[-1], "w").close())

    export.export_dataset_releases("ds")

    linked = {r: (tmp_path / f"release_{r}" / export_filename("ds")).resolve() for r in (30, 66)}
    assert linked[30] == linked[66]
    assert linked[30].name != export_filename("empty")


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
