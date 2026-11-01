from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ..config import Source
from .pull import pull_repo

DATASET = "test_dataset"
MODULE = "kart_import.assets.pull"


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
    monkeypatch.setattr(f"{MODULE}.env_skip_source_pull", lambda: False)

    return mocks


def _cmds(env) -> list[list[str]]:
    return [c.args[0] for c in env.run_command.call_args_list]


def test_missing_local_clone_is_skipped(env):
    pull_repo(DATASET)

    assert not any(cmd[:2] == ["kart", "pull"] for cmd in _cmds(env))
    assert (env.target_dir / ".pulled").exists()


def test_existing_clone_always_pulls_directly_from_source(env):
    """`pull_all` ignores published bundles entirely and always pulls straight from source."""
    (env.target_dir / ".kart").mkdir(parents=True)

    pull_repo(DATASET)

    cmds = _cmds(env)
    assert ["git", "remote", "set-url", "origin", "kart@example.com:linz/test"] in cmds
    assert ["kart", "pull"] in cmds
    assert (env.target_dir / ".pulled").exists()


def test_unknown_repo_raises(env):
    with pytest.raises(LookupError, match="No source repo"):
        pull_repo("does_not_exist")


def test_skips_source_pull_when_stubbed(env, monkeypatch):
    """KART_IMPORT_SKIP_SOURCE_PULL trusts the existing clone as-is, for environments (e2e)
    with no credentials to reach the real source."""
    (env.target_dir / ".kart").mkdir(parents=True)
    monkeypatch.setattr(f"{MODULE}.env_skip_source_pull", lambda: True)

    pull_repo(DATASET)

    assert not any(cmd[:2] == ["kart", "pull"] for cmd in _cmds(env))
    assert (env.target_dir / ".pulled").exists()
