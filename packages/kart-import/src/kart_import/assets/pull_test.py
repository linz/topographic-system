from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ..config import Source
from .pull import pull_repo

SHA = "a" * 40  # local clone is up to date with the published bundle
SHA_OLD = "b" * 40  # local clone's HEAD, now behind the published bundle
DATASET = "test_dataset"
MODULE = "kart_import.assets.pull"


@pytest.fixture
def env(tmp_path, monkeypatch):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    target_dir = source_dir / DATASET

    state = SimpleNamespace(local_head=SHA)

    def run_command(cmd, cwd=None, **kwargs):
        if cmd[:2] == ["git", "rev-parse"]:
            return f"{state.local_head}\n"
        return ""

    mocks = SimpleNamespace(
        target_dir=target_dir,
        state=state,
        run_command=MagicMock(side_effect=run_command),
        fetch_bundle_head=MagicMock(return_value=SHA),
    )

    monkeypatch.setattr(f"{MODULE}.SOURCE_DIR", source_dir)
    repos = {DATASET: [SimpleNamespace(source=Source(url="kart@example.com:linz/test"))]}

    def get_source_repo(name):
        if name not in repos:
            raise LookupError(f"No source repo named {name!r}")
        return repos[name]

    monkeypatch.setattr(f"{MODULE}.get_source_repo", get_source_repo)
    monkeypatch.setattr(f"{MODULE}.run_command", mocks.run_command)
    monkeypatch.setattr(f"{MODULE}.fetch_bundle_head", mocks.fetch_bundle_head)

    return mocks


def _cmds(env) -> list[list[str]]:
    return [c.args[0] for c in env.run_command.call_args_list]


def test_missing_local_clone_is_skipped(env):
    pull_repo(DATASET)

    assert not any(cmd[:2] == ["kart", "pull"] for cmd in _cmds(env))
    assert (env.target_dir / ".pulled").exists()


def test_clone_matching_bundle_is_left_alone(env):
    (env.target_dir / ".kart").mkdir(parents=True)
    env.fetch_bundle_head.return_value = SHA
    env.state.local_head = SHA

    pull_repo(DATASET)

    assert not any(cmd[:2] == ["kart", "pull"] for cmd in _cmds(env))
    assert (env.target_dir / ".pulled").exists()


def test_clone_behind_bundle_pulls_from_source(env):
    (env.target_dir / ".kart").mkdir(parents=True)
    env.fetch_bundle_head.return_value = SHA
    env.state.local_head = SHA_OLD

    pull_repo(DATASET)

    cmds = _cmds(env)
    assert ["git", "remote", "set-url", "origin", "kart@example.com:linz/test"] in cmds
    assert ["kart", "pull"] in cmds
    assert (env.target_dir / ".pulled").exists()


def test_bundle_head_unknown_still_pulls(env):
    """A bundle that can't be reached (`fetch_bundle_head` returns None) can't confirm
    freshness, so pull defensively rather than silently trusting the local clone."""
    (env.target_dir / ".kart").mkdir(parents=True)
    env.fetch_bundle_head.return_value = None
    env.state.local_head = SHA

    pull_repo(DATASET)

    assert ["kart", "pull"] in _cmds(env)


def test_unknown_repo_raises(env):
    with pytest.raises(LookupError, match="No source repo"):
        pull_repo("does_not_exist")
