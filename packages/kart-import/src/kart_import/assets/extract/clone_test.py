from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from .clone import clone_repo

REPO = "topographic-source-data"
MODULE = "kart_import.assets.extract.clone"


def test_clone_repo_clones_no_checkout_and_touches_sentinel(monkeypatch, tmp_path):
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

    clone_repo(REPO)

    rc.assert_called_once_with(
        ["git", "clone", "git@github.com:linz/topographic-source-data", str(target_dir), "--no-checkout"]
    )
    assert (target_dir / ".cloned").exists()
