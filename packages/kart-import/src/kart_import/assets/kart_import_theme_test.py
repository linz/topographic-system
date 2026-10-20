"""Exercises `kart_import_theme` against the real `kart`/`git` binaries rather than mocking
`run_command`: the parallel-import path writes real objects through `GIT_OBJECT_DIRECTORY`
redirection and reads them back with real `git` plumbing, none of which a mocked subprocess layer
can stand in for.
"""

import subprocess
from datetime import datetime
from pathlib import Path

import geopandas as gpd
from shapely.geometry import Point

from ..config import Release
from . import kart_import_theme as kit
from .kart_import_theme import THEME_SUFFIX, kart_import_theme

THEME = "test_theme"


def _releases(*ids: int) -> list[Release]:
    return [Release(id=i, date=datetime(2020, 1, 1), until=datetime(2020, 1, i + 1)) for i in ids]


def _write_fgb(working_theme_dir: Path, theme: str, release_id: int, ids: list[int]) -> None:
    """A tiny valid theme fgb: an `id` primary key plus a value column that encodes the release,
    so two releases only produce identical content when deliberately given the same `ids`."""
    release_dir = working_theme_dir / f"release_{release_id}"
    release_dir.mkdir(parents=True, exist_ok=True)
    path = release_dir / f"{theme}{THEME_SUFFIX}"
    gdf = gpd.GeoDataFrame(
        {"id": [str(i) for i in ids], "value": [f"v{i}" for i in ids]},
        geometry=[Point(i, i) for i in ids],
        crs="EPSG:4326",
    )
    gdf.to_file(path, driver="FlatGeobuf", index=False)


def _setup(monkeypatch, tmp_path):
    output_dir = tmp_path / "output"
    working_theme_dir = tmp_path / "working" / "theme"
    monkeypatch.setattr(kit, "OUTPUT_DIR", output_dir)
    monkeypatch.setattr(kit, "WORKING_THEME_DIR", working_theme_dir)
    monkeypatch.setattr(kit, "WORKER_DIR", tmp_path / "workers")
    return output_dir, working_theme_dir


def _release_refs(repo_dir: Path) -> dict[str, str]:
    """Every `refs/heads/release/<id>` ref in the theme repo, as {id: commit sha}."""
    out = subprocess.run(
        ["git", "for-each-ref", "--format=%(refname) %(objectname)", "refs/heads/release/"],
        cwd=repo_dir,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return {line.split()[0].rsplit("/", 1)[1]: line.split()[1] for line in out.splitlines() if line}


def _tree(repo_dir: Path, commit: str) -> str:
    return subprocess.run(
        ["git", "rev-parse", f"{commit}^{{tree}}"], cwd=repo_dir, capture_output=True, text=True, check=True
    ).stdout.strip()


def _schema(repo_dir: Path, commit: str, theme: str) -> str:
    return subprocess.run(
        ["git", "show", f"{commit}:{theme}/.table-dataset/meta/schema.json"],
        cwd=repo_dir,
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def test_every_release_lands_as_its_own_ref_sharing_one_schema(monkeypatch, tmp_path):
    output_dir, working_theme_dir = _setup(monkeypatch, tmp_path)
    releases = _releases(1, 2, 3)
    monkeypatch.setattr(kit, "get_releases", lambda: releases)
    ids_by_release = {1: [1], 2: [1, 2], 3: [1, 2, 3]}
    for release_id, ids in ids_by_release.items():
        _write_fgb(working_theme_dir, THEME, release_id, ids)

    kart_import_theme(THEME, thread_count=4)

    repo_dir = output_dir / "theme" / THEME
    refs = _release_refs(repo_dir)
    assert set(refs) == {"1", "2", "3"}

    # Every release's commit reused the same schema -- same column UUIDs -- even though each was
    # imported by its own independent, parallel worker.
    schemas = {
        subprocess.run(
            ["git", "ls-tree", "-r", sha, "--", f"{THEME}/.table-dataset/meta/schema.json"],
            cwd=repo_dir,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        for sha in refs.values()
    }
    assert len(schemas) == 1

    # Feature counts match what was written for each release.
    for release_id, ids in ids_by_release.items():
        count = subprocess.run(
            ["git", "ls-tree", "-r", refs[str(release_id)], "--", f"{THEME}/.table-dataset/feature"],
            cwd=repo_dir,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        assert len(count.splitlines()) == len(ids)


def test_identical_adjacent_releases_collapse_to_one_ref(monkeypatch, tmp_path):
    """Releases 2 and 3 carry identical content. Each is imported independently by its own
    parallel worker (forked from the same empty schema stub, not from each other), so this checks
    that the post-hoc tree-sha dedup pass still collapses them into a single surviving ref --
    matching what a sequential `kart import --replace-existing` would do by reporting "No changes
    to commit" itself."""
    output_dir, working_theme_dir = _setup(monkeypatch, tmp_path)
    monkeypatch.setattr(kit, "get_releases", lambda: _releases(1, 2, 3, 4))
    ids_by_release = {1: [1], 2: [1, 2], 3: [1, 2], 4: [1, 2, 3]}
    for release_id, ids in ids_by_release.items():
        _write_fgb(working_theme_dir, THEME, release_id, ids)

    kart_import_theme(THEME, thread_count=4)

    repo_dir = output_dir / "theme" / THEME
    refs = _release_refs(repo_dir)
    assert set(refs) == {"1", "2", "4"}  # release 3 collapsed into release 2
    assert _tree(repo_dir, refs["2"]) != _tree(repo_dir, refs["1"])
    assert _tree(repo_dir, refs["4"]) != _tree(repo_dir, refs["2"])


def test_two_themes_get_independent_repos_and_schemas(monkeypatch, tmp_path):
    output_dir, working_theme_dir = _setup(monkeypatch, tmp_path)
    releases = _releases(1, 2)
    monkeypatch.setattr(kit, "get_releases", lambda: releases)
    _write_fgb(working_theme_dir, "theme_a", 1, [1])
    _write_fgb(working_theme_dir, "theme_a", 2, [1, 2])
    _write_fgb(working_theme_dir, "theme_b", 1, [9])
    _write_fgb(working_theme_dir, "theme_b", 2, [9, 8])

    kart_import_theme("theme_a", thread_count=4)
    kart_import_theme("theme_b", thread_count=4)

    repo_a = output_dir / "theme" / "theme_a"
    repo_b = output_dir / "theme" / "theme_b"
    assert set(_release_refs(repo_a)) == {"1", "2"}
    assert set(_release_refs(repo_b)) == {"1", "2"}
    assert (output_dir / "theme_a.bundle").exists()
    assert (output_dir / "theme_b.bundle").exists()


def test_rerun_rebuilds_the_theme_repo_from_scratch(monkeypatch, tmp_path):
    output_dir, working_theme_dir = _setup(monkeypatch, tmp_path)
    monkeypatch.setattr(kit, "get_releases", lambda: _releases(1, 2))
    _write_fgb(working_theme_dir, THEME, 1, [1])
    _write_fgb(working_theme_dir, THEME, 2, [1, 2])

    kart_import_theme(THEME, thread_count=4)
    repo_dir = output_dir / "theme" / THEME
    first_run_refs = _release_refs(repo_dir)

    kart_import_theme(THEME, thread_count=4)
    second_run_refs = _release_refs(repo_dir)

    # Not asserting commit sha equality (dates/messages are the same but git commit shas are not
    # content-addressed on those alone) -- just that a second run produces a complete, correct
    # repo again rather than erroring on leftover state.
    assert set(second_run_refs) == set(first_run_refs) == {"1", "2"}


def test_schema_ids_are_stable_across_independent_rebuilds(monkeypatch, tmp_path):
    """The whole point of `assign_stable_column_ids`: column ids must match across two separate
    pipeline runs, not just across workers within one run -- otherwise `kart diff`/`kart log`
    would show every rebuild as "entire dataset replaced" instead of the real, small change."""
    output_dir, working_theme_dir = _setup(monkeypatch, tmp_path)
    monkeypatch.setattr(kit, "get_releases", lambda: _releases(1, 2))
    _write_fgb(working_theme_dir, THEME, 1, [1])
    _write_fgb(working_theme_dir, THEME, 2, [1, 2])
    repo_dir = output_dir / "theme" / THEME

    kart_import_theme(THEME, thread_count=4)
    first_run_refs = _release_refs(repo_dir)
    first_run_schema = _schema(repo_dir, first_run_refs["1"], THEME)

    kart_import_theme(THEME, thread_count=4)
    second_run_refs = _release_refs(repo_dir)
    second_run_schema = _schema(repo_dir, second_run_refs["1"], THEME)

    assert first_run_schema == second_run_schema
