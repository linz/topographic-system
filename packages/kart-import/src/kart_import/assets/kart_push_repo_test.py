import logging
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ..config import Release
from .kart_push_repo import push_repo, release_branch, release_checkpoints, release_tag

REPO = "topographic-data"
URL = "git@github.com:linz/topographic-data"
MODULE = "kart_import.assets.kart_push_repo"

# Two releases, two themes each, each closed by its `linearise` marker. Oldest commit first.
LOG = (
    "aaa1 import airport for release 64\n"
    "aaa2 import road_line for release 64\n"
    "aaa3 release 64\n"
    "bbb1 import airport for release 66\n"
    "bbb2 import road_line for release 66\n"
    "bbb3 release 66\n"
)


def _releases(*ids: int) -> list[Release]:
    return [Release(id=i, date=datetime(2020, 1, 1)) for i in ids]


def _setup(monkeypatch, tmp_path, *, built: bool = True, log: str = LOG):
    output_dir = tmp_path / "output"
    repo_dir = output_dir / REPO
    if built:
        (repo_dir / ".git").mkdir(parents=True)

    rc = MagicMock(side_effect=lambda cmd, **kwargs: log if cmd[:2] == ["git", "log"] else "")
    monkeypatch.setattr(f"{MODULE}.OUTPUT_DIR", output_dir)
    monkeypatch.setattr(f"{MODULE}.run_command", rc)
    monkeypatch.setattr(f"{MODULE}.get_repo_remote", lambda name: URL)
    monkeypatch.setattr(f"{MODULE}.get_releases", lambda: _releases(64, 66))
    return SimpleNamespace(repo_dir=repo_dir, run_command=rc)


def _cmds(rc) -> list[list[str]]:
    return [c.args[0] for c in rc.call_args_list]


def _pushes(rc) -> list[list[str]]:
    return [cmd for cmd in _cmds(rc) if cmd[:2] == ["git", "push"]]


def test_release_branch_uses_latest_release(monkeypatch):
    monkeypatch.setattr(f"{MODULE}.get_releases", lambda: _releases(64, 66))
    assert release_branch() == "feat/release66"


def test_release_branch_falls_back_to_import_without_releases(monkeypatch):
    monkeypatch.setattr(f"{MODULE}.get_releases", lambda: [])
    assert release_branch() == "import"


def test_release_checkpoints_takes_each_release_marker(monkeypatch, tmp_path):
    """The marker closes its release, so the branch advances exactly one release per push."""
    env = _setup(monkeypatch, tmp_path)

    assert release_checkpoints(env.repo_dir) == [(64, "aaa3"), (66, "bbb3")]


def test_release_checkpoints_ignores_theme_commits(monkeypatch, tmp_path):
    """ "import <theme> for release <id>" ends in a release too, but is not where one ends."""
    env = _setup(monkeypatch, tmp_path, log="aaa1 import airport for release 64\n")

    with pytest.raises(ValueError, match="No release markers found"):
        release_checkpoints(env.repo_dir)


def test_release_checkpoints_rejects_a_history_with_no_releases(monkeypatch, tmp_path):
    env = _setup(monkeypatch, tmp_path, log="aaa1 some unrelated commit\n")

    with pytest.raises(ValueError, match="No release markers found"):
        release_checkpoints(env.repo_dir)


def test_push_repo_raises_when_repo_not_built(monkeypatch, tmp_path):
    env = _setup(monkeypatch, tmp_path, built=False)

    with pytest.raises(RuntimeError, match="Target repo not built"):
        push_repo(REPO)

    assert env.run_command.call_count == 0  # bailed before touching git


def test_push_repo_pushes_one_release_at_a_time(monkeypatch, tmp_path):
    """A single push of the whole history exceeds GitHub's pack limit; releases go up in order."""
    env = _setup(monkeypatch, tmp_path)

    ref = push_repo(REPO)

    assert ref == "feat/release66"
    assert _pushes(env.run_command) == [
        ["git", "push", "origin", "aaa3:refs/heads/feat/release66", "refs/tags/release/64"],
        ["git", "push", "origin", "bbb3:refs/heads/feat/release66", "refs/tags/release/66"],
    ]
    assert (env.repo_dir / ".pushed").read_text() == f"{URL} feat/release66\n"


def test_push_repo_tags_each_release_before_pushing_it(monkeypatch, tmp_path):
    env = _setup(monkeypatch, tmp_path)

    push_repo(REPO)

    cmds = _cmds(env.run_command)
    assert ["git", "tag", "--force", "release/64", "aaa3"] in cmds
    assert ["git", "tag", "--force", "release/66", "bbb3"] in cmds
    # The tag has to exist locally before the push that carries it.
    assert cmds.index(["git", "tag", "--force", "release/64", "aaa3"]) < cmds.index(_pushes(env.run_command)[0])


def test_push_repo_sets_up_the_remote_before_pushing(monkeypatch, tmp_path):
    env = _setup(monkeypatch, tmp_path)

    push_repo(REPO)

    assert _cmds(env.run_command)[:2] == [
        ["git", "remote", "remove", "origin"],
        ["git", "remote", "add", "origin", URL],
    ]


def test_push_repo_pushes_to_master(monkeypatch, tmp_path):
    env = _setup(monkeypatch, tmp_path)

    ref = push_repo(REPO, to_master=True)

    assert ref == "master"
    assert ["git", "push", "origin", "bbb3:refs/heads/master", "refs/tags/release/66"] in _cmds(env.run_command)
    assert (env.repo_dir / ".pushed").read_text() == f"{URL} master\n"


def test_push_repo_force_adds_force_flag_to_every_release(monkeypatch, tmp_path):
    """The first push rewrites the branch; the rest fast-forward, but a moved tag still needs it."""
    env = _setup(monkeypatch, tmp_path)

    push_repo(REPO, force=True)

    assert all(cmd[2] == "--force" for cmd in _pushes(env.run_command))


def test_push_repo_reports_progress_per_release(monkeypatch, tmp_path, caplog):
    """`snake.bash` renders these into a progress line with a rate and an ETA."""
    _setup(monkeypatch, tmp_path)
    # caplog captures at the root logger; `kart_import` sets propagate=False, so switch it back on.
    monkeypatch.setattr(logging.getLogger("kart_import"), "propagate", True)

    with caplog.at_level("INFO", logger="kart_import"):
        push_repo(REPO)

    chunks = [r for r in caplog.records if r.msg == "pushing chunk"]
    assert [(r.push, r.of) for r in chunks] == [(64, 66), (66, 66)]


def test_release_tag_names_the_release():
    assert release_tag(66) == "release/66"
