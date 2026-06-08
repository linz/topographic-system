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


def test_make_lifecycle_id_namespaces_non_t50_fid_by_dataset():
    """t50_fid is global; auto_pk and any configured key are per-dataset, so the
    same raw fid in two datasets must yield different ids."""
    commit_time = "2015-03-15T00:00:00+00:00"

    t50_a = fid_lifecycle.make_lifecycle_id(commit_time, "123", "t50_fid", "dataset_a")
    t50_b = fid_lifecycle.make_lifecycle_id(commit_time, "123", "t50_fid", "dataset_b")
    assert t50_a == t50_b  # dataset id is irrelevant for t50_fid

    for field in ("auto_pk", "lol_sufi"):
        id_a = fid_lifecycle.make_lifecycle_id(commit_time, "123", field, "dataset_a")
        id_b = fid_lifecycle.make_lifecycle_id(commit_time, "123", field, "dataset_b")
        assert id_a != id_b, f"{field} should be namespaced by dataset id"
