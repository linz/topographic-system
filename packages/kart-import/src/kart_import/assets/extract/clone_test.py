from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from .clone import clone_repo, clone_sentinel_name

REPO = "topographic-source-data"
MODULE = "kart_import.assets.extract.clone"
TODAY = date(2026, 11, 20)


def _setup(monkeypatch, tmp_path):
    source_dir = tmp_path / "source"
    target_dir = source_dir / REPO

    def run_command(cmd, cwd=None, **kwargs):
        if cmd[:2] == ["git", "clone"]:
            Path(cmd[3]).mkdir(parents=True, exist_ok=True)
        return ""

    rc = MagicMock(side_effect=run_command)
    monkeypatch.setattr(f"{MODULE}.SOURCE_DIR", source_dir)
    monkeypatch.setattr(f"{MODULE}.run_command", rc)
    monkeypatch.setattr(
        f"{MODULE}.get_source_repo",
        lambda name: [SimpleNamespace(source=SimpleNamespace(url="git@github.com:linz/topographic-source-data"))],
    )
    return target_dir, rc


def test_clone_repo_clones_no_checkout_and_touches_daily_sentinel(monkeypatch, tmp_path):
    target_dir, rc = _setup(monkeypatch, tmp_path)

    clone_repo(REPO, day=TODAY)

    rc.assert_called_once_with(
        ["git", "clone", "git@github.com:linz/topographic-source-data", str(target_dir), "--no-checkout"]
    )
    assert (target_dir / clone_sentinel_name(TODAY)).exists()


def test_clone_repo_fetches_existing_clone_and_cleans_old_sentinels(monkeypatch, tmp_path):
    target_dir, rc = _setup(monkeypatch, tmp_path)
    (target_dir / ".git").mkdir(parents=True)
    old_sentinel = target_dir / ".cloned_2026-11-19"
    old_sentinel.touch()
    legacy_sentinel = target_dir / ".cloned"
    legacy_sentinel.touch()

    clone_repo(REPO, day=TODAY)

    rc.assert_called_once_with(["git", "fetch", "origin"], cwd=str(target_dir))
    assert (target_dir / ".cloned_2026-11-20").exists()
    assert not old_sentinel.exists()
    assert not legacy_sentinel.exists()
