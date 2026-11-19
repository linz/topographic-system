from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ..config import Source, ThemeDataset
from .pull import invalidate_stale_transforms, pull_repo

DATASET = "test_dataset"
MODULE = "kart_import.assets.pull"


def _dataset(name: str) -> ThemeDataset:
    return ThemeDataset.model_validate({"name": name, "source": "kart@data.koordinates.com:linz/x-topo-150k"})


@pytest.fixture
def env(tmp_path, monkeypatch):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    target_dir = source_dir / DATASET

    def run_command(cmd, cwd=None, **kwargs):
        return ""

    mocks = SimpleNamespace(
        target_dir=target_dir,
        run_command=MagicMock(side_effect=run_command),
    )

    monkeypatch.setattr(f"{MODULE}.SOURCE_DIR", source_dir)
    repos = {DATASET: [SimpleNamespace(source=Source(url="kart@example.com:linz/test"))]}

    def get_source_repo(name):
        if name not in repos:
            raise LookupError(f"No source repo named {name!r}")
        return repos[name]

    monkeypatch.setattr(f"{MODULE}.get_source_repo", get_source_repo)
    monkeypatch.setattr(f"{MODULE}.run_command", mocks.run_command)
    monkeypatch.setattr(f"{MODULE}.release_commit_map", lambda name: {})

    return mocks


def _cmds(env) -> list[list[str]]:
    return [c.args[0] for c in env.run_command.call_args_list]


def test_missing_local_clone_is_skipped(env):
    pull_repo(DATASET)

    assert not any(cmd[:2] == ["kart", "pull"] for cmd in _cmds(env))
    assert (env.target_dir / ".pulled").exists()


def test_existing_clone_always_pulls_directly_from_source(env):
    """`pull_all` always pulls straight from source."""
    (env.target_dir / ".kart").mkdir(parents=True)

    pull_repo(DATASET)

    cmds = _cmds(env)
    assert ["git", "remote", "set-url", "origin", "kart@example.com:linz/test"] in cmds
    assert ["kart", "pull"] in cmds
    assert (env.target_dir / ".pulled").exists()


def test_unknown_repo_raises(env):
    with pytest.raises(LookupError, match="No source repo"):
        pull_repo("does_not_exist")


def _write(path, text="x"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def test_invalidate_stale_transforms_deletes_only_releases_whose_commit_moved(tmp_path, monkeypatch):
    transform_dir = tmp_path / "transform"
    monkeypatch.setattr(f"{MODULE}.WORKING_TRANSFORM_DIR", transform_dir)
    monkeypatch.setattr(f"{MODULE}.get_source_repo", lambda name: [_dataset(DATASET)])
    monkeypatch.setattr(f"{MODULE}.release_commit_map", lambda name: {30: "a", 67: "new"})

    moved_file = transform_dir / "release_67" / f"{DATASET}.parquet"
    stable_file = transform_dir / "release_30" / f"{DATASET}.parquet"
    _write(moved_file, "stale")
    _write(stable_file, "fresh")

    invalidate_stale_transforms(DATASET, before={DATASET: {30: "a", 67: "old"}})

    assert not moved_file.exists()
    assert stable_file.read_text() == "fresh"


def test_invalidate_stale_transforms_treats_a_newly_available_release_as_changed(tmp_path, monkeypatch):
    """A release that used to predate the dataset's first commit (`None`) and now resolves to
    one is a change too, not just a commit swapping for another commit."""
    transform_dir = tmp_path / "transform"
    monkeypatch.setattr(f"{MODULE}.WORKING_TRANSFORM_DIR", transform_dir)
    monkeypatch.setattr(f"{MODULE}.get_source_repo", lambda name: [_dataset(DATASET)])
    monkeypatch.setattr(f"{MODULE}.release_commit_map", lambda name: {30: "a"})

    stale_empty_file = transform_dir / "release_30" / f"{DATASET}.parquet"
    _write(stale_empty_file, "empty-placeholder")

    invalidate_stale_transforms(DATASET, before={DATASET: {30: None}})

    assert not stale_empty_file.exists()


def test_invalidate_stale_transforms_skips_lookups(tmp_path, monkeypatch):
    """A lookup sourced from the same repo has its own per-commit cache - never touched here."""
    transform_dir = tmp_path / "transform"
    monkeypatch.setattr(f"{MODULE}.WORKING_TRANSFORM_DIR", transform_dir)
    monkeypatch.setattr(f"{MODULE}.get_source_repo", lambda name: [SimpleNamespace(name="a_lookup")])
    monkeypatch.setattr(
        f"{MODULE}.release_commit_map", lambda name: pytest.fail(f"lookup {name!r} should be skipped")
    )

    invalidate_stale_transforms(DATASET, before={})


def test_pull_invalidates_only_the_release_that_moved(env, tmp_path, monkeypatch):
    """End to end through `pull_repo`: a release resolved before the pull with one commit and
    after with another loses its stale transform; an unaffected release keeps its file."""
    (env.target_dir / ".kart").mkdir(parents=True)

    transform_dir = tmp_path / "transform"
    monkeypatch.setattr(f"{MODULE}.WORKING_TRANSFORM_DIR", transform_dir)
    monkeypatch.setattr(f"{MODULE}.get_source_repo", lambda name: [_dataset(DATASET)])

    moved_file = transform_dir / "release_67" / f"{DATASET}.parquet"
    stable_file = transform_dir / "release_30" / f"{DATASET}.parquet"
    _write(moved_file, "stale")
    _write(stable_file, "fresh")

    commit_maps = iter([{30: "a", 67: "old"}, {30: "a", 67: "new"}])
    monkeypatch.setattr(f"{MODULE}.release_commit_map", lambda name: next(commit_maps))

    pull_repo(DATASET)

    assert not moved_file.exists()
    assert stable_file.read_text() == "fresh"
