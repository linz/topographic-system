import json
from pathlib import Path

from .. import config
from ..config import Source, ThemeDataset
from . import fid_lifecycle


def test_resolve_dataset_id_prefers_explicit_source_dataset(monkeypatch):
    """A multi-dataset repo must use the configured dataset id, never auto-detect."""
    td = ThemeDataset(
        name="lamps_linz_road_cl",
        source=Source(url="git@github.com:linz/topographic-source-data", dataset="linz_road_cl"),
    )
    monkeypatch.setitem(config.DATASET_MAP, td.name, td)

    def _boom(_repo_dir):
        raise AssertionError("get_kart_dataset_id must not be called when source.dataset is set")

    monkeypatch.setattr(fid_lifecycle, "get_kart_dataset_id", _boom)

    assert fid_lifecycle.resolve_dataset_id(td.name, Path("/tmp/repo")) == "linz_road_cl"


def test_resolve_dataset_id_falls_back_to_autodetect(monkeypatch):
    """A single-dataset koordinates repo has no explicit dataset; auto-detect it."""
    td = ThemeDataset(
        name="nz_airport_polygons",
        source=Source(url="kart@data.koordinates.com:linz/nz-airport-polygons-topo-150k"),
    )
    monkeypatch.setitem(config.DATASET_MAP, td.name, td)
    monkeypatch.setattr(fid_lifecycle, "get_kart_dataset_id", lambda _repo_dir: "nz-airport-polygons-topo-150k")

    assert fid_lifecycle.resolve_dataset_id(td.name, Path("/tmp/repo")) == "nz-airport-polygons-topo-150k"


def _insert(change: dict) -> str:
    return json.dumps({"type": "feature", "dataset": "ds", "change": {"++": change}})


def test_parse_kart_diff_reads_auto_pk_renamed_to_fid():
    """topographic-source-data renamed auto_pk to fid in its release 67 reload: rows before and after
    the rename are both keyed."""
    lifecycle: dict = {}
    fid_lifecycle.parse_kart_diff(_insert({"auto_pk": 128506}), lifecycle, "2026-09-14T00:00:00+00:00", "ds", "auto_pk")
    fid_lifecycle.parse_kart_diff(_insert({"fid": 1}), lifecycle, "2026-10-19T00:00:00+00:00", "ds", "auto_pk")

    assert {fid: entry["created_at"] for fid, entry in lifecycle.items()} == {
        "128506": "2026-09-14T00:00:00+00:00",
        "1": "2026-10-19T00:00:00+00:00",
    }
