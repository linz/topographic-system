from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ..config import Release
from .kart_push_repo import push_repo, release_branch

REPO = "topographic-data"
URL = "git@github.com:linz/topographic-data"
MODULE = "kart_import.assets.kart_push_repo"


def _releases(*ids: int) -> list[Release]:
    return [Release(id=i, date=datetime(2020, 1, 1)) for i in ids]


def _setup(monkeypatch, tmp_path, *, built: bool = True):
    output_dir = tmp_path / "output"
    repo_dir = output_dir / REPO
    if built:
        (repo_dir / ".git").mkdir(parents=True)

    rc = MagicMock(return_value="")
    monkeypatch.setattr(f"{MODULE}.OUTPUT_DIR", output_dir)
    monkeypatch.setattr(f"{MODULE}.run_command", rc)
    monkeypatch.setattr(f"{MODULE}.get_repo_remote", lambda name: URL)
    monkeypatch.setattr(f"{MODULE}.get_releases", lambda: _releases(64, 66))
    return SimpleNamespace(repo_dir=repo_dir, run_command=rc)


def _cmds(rc) -> list[list[str]]:
    return [c.args[0] for c in rc.call_args_list]


def test_release_branch_uses_latest_release(monkeypatch):
    monkeypatch.setattr(f"{MODULE}.get_releases", lambda: _releases(64, 66))
    assert release_branch() == "feat/release66"


def test_release_branch_falls_back_to_import_without_releases(monkeypatch):
    monkeypatch.setattr(f"{MODULE}.get_releases", lambda: [])
    assert release_branch() == "import"


def test_push_repo_raises_when_repo_not_built(monkeypatch, tmp_path):
    env = _setup(monkeypatch, tmp_path, built=False)

    with pytest.raises(RuntimeError, match="Target repo not built"):
        push_repo(REPO)

    assert env.run_command.call_count == 0  # bailed before touching git


def test_push_repo_pushes_to_release_branch_by_default(monkeypatch, tmp_path):
    env = _setup(monkeypatch, tmp_path)

    ref = push_repo(REPO)

    assert ref == "feat/release66"
    # Exact sequence: stale origin cleared, re-pointed at the remote, then pushed.
    assert _cmds(env.run_command) == [
        ["git", "remote", "remove", "origin"],
        ["git", "remote", "add", "origin", URL],
        ["git", "push", "origin", "HEAD:refs/heads/feat/release66"],
    ]
    assert (env.repo_dir / ".pushed").read_text() == f"{URL} feat/release66\n"


def test_push_repo_pushes_to_master(monkeypatch, tmp_path):
    env = _setup(monkeypatch, tmp_path)

    ref = push_repo(REPO, to_master=True)

    assert ref == "master"
    assert ["git", "push", "origin", "HEAD:refs/heads/master"] in _cmds(env.run_command)
    assert (env.repo_dir / ".pushed").read_text() == f"{URL} master\n"


def test_push_repo_force_adds_force_flag(monkeypatch, tmp_path):
    env = _setup(monkeypatch, tmp_path)

    push_repo(REPO, force=True)

    assert ["git", "push", "--force", "origin", "HEAD:refs/heads/feat/release66"] in _cmds(env.run_command)
