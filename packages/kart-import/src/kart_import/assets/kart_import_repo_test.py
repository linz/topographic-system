"""Exercises `kart_import_repo` end-to-end against real theme repos built by `kart_import_theme`,
since it reads directly from the bundles that function writes."""

import subprocess
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import geopandas as gpd
from shapely.geometry import Point

from ..config import Release
from . import kart_import_repo as kir
from . import kart_import_theme as kit
from .kart_import_repo import kart_import_repo
from .kart_import_theme import THEME_SUFFIX, kart_import_theme

REPO = "test_repo"


def git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=str(repo), capture_output=True, text=True, check=True).stdout.strip()


def _releases(*ids: int) -> list[Release]:
    return [Release(id=i, date=datetime(2020, 1, 1), until=datetime(2020, 1, i + 1)) for i in ids]


def _write_fgb(working_theme_dir: Path, theme: str, release_id: int, ids: list[int]) -> None:
    release_dir = working_theme_dir / f"release_{release_id}"
    release_dir.mkdir(parents=True, exist_ok=True)
    path = release_dir / f"{theme}{THEME_SUFFIX}"
    gdf = gpd.GeoDataFrame(
        {"id": [str(i) for i in ids], "value": [f"v{i}" for i in ids]},
        geometry=[Point(i, i) for i in ids],
        crs="EPSG:4326",
    )
    gdf.to_file(path, driver="FlatGeobuf", index=False)


def _setup(monkeypatch, tmp_path, theme_names: list[str]):
    output_dir = tmp_path / "output"
    working_theme_dir = tmp_path / "working" / "theme"
    monkeypatch.setattr(kit, "OUTPUT_DIR", output_dir)
    monkeypatch.setattr(kit, "WORKING_THEME_DIR", working_theme_dir)
    monkeypatch.setattr(kit, "WORKER_DIR", tmp_path / "workers")
    monkeypatch.setattr(kir, "OUTPUT_DIR", output_dir)
    themes = [SimpleNamespace(name=name, target_repo=REPO) for name in theme_names]
    monkeypatch.setattr(kir, "get_themes", lambda: themes)
    return output_dir, working_theme_dir


def test_kart_import_repo_combines_two_themes_chronologically(monkeypatch, tmp_path):
    output_dir, working_theme_dir = _setup(monkeypatch, tmp_path, ["airport", "island"])
    monkeypatch.setattr(kit, "get_releases", lambda: _releases(1, 2))
    _write_fgb(working_theme_dir, "airport", 1, [1])
    _write_fgb(working_theme_dir, "airport", 2, [1, 2])
    _write_fgb(working_theme_dir, "island", 1, [9])
    _write_fgb(working_theme_dir, "island", 2, [9, 8])

    kart_import_theme("airport", thread_count=4)
    kart_import_theme("island", thread_count=4)

    kart_import_repo(REPO)

    repo_dir = output_dir / REPO
    assert (repo_dir / ".imported").exists()

    log = git(repo_dir, "log", "--reverse", "--format=%s", "master")
    subjects = log.splitlines()
    # 2 releases x 2 themes = 4 import commits, plus a `release <id>` boundary marker closing
    # each release.
    assert subjects.count("release 1") == 1
    assert subjects.count("release 2") == 1
    assert "import airport for release 1" in subjects
    assert "import island for release 1" in subjects
    assert "import airport for release 2" in subjects
    assert "import island for release 2" in subjects

    # Both themes' datasets are present side by side at the tip.
    tree = git(repo_dir, "ls-tree", "master")
    assert "airport" in tree
    assert "island" in tree

    # The schema-stub commits and temporary per-theme fetched refs are not left lying around.
    assert git(repo_dir, "for-each-ref", "--format=%(refname)", "refs/heads/") == "refs/heads/master"


def test_kart_import_repo_requires_bundles_to_exist(monkeypatch, tmp_path):
    _setup(monkeypatch, tmp_path, ["airport"])

    try:
        kart_import_repo(REPO)
        assert False, "expected an exception for a missing bundle"
    except Exception as e:
        assert "Bundle file not found" in str(e)


def test_kart_import_repo_warns_and_returns_for_an_unknown_repo(monkeypatch, tmp_path):
    _setup(monkeypatch, tmp_path, ["airport"])

    assert kart_import_repo("some_other_repo") is None
