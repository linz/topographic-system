from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from .clone import clone_dataset

DATASET = "testmapsheet"
MODULE = "kart_import.assets.clone"


def _setup(monkeypatch, tmp_path, *, has_dataset: bool):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    target_dir = source_dir / DATASET

    def run_command(cmd, cwd=None, **kwargs):
        if cmd[:2] == ["git", "clone"]:
            Path(cmd[3]).mkdir(parents=True, exist_ok=True)  # real git clone creates the dir
        return ""

    rc = MagicMock(side_effect=run_command)
    monkeypatch.setattr(f"{MODULE}.SOURCE_DIR", source_dir)
    monkeypatch.setattr(f"{MODULE}.run_command", rc)
    monkeypatch.setattr(f"{MODULE}.env_use_bundle", lambda: False)  # exercise the direct-clone path
    monkeypatch.setattr(
        f"{MODULE}.get_source_entry",
        lambda name: SimpleNamespace(
            source=SimpleNamespace(url="git@github.com:linz/topographic-source-data", dataset="linz_map_sheet")
        ),
    )
    monkeypatch.setattr(f"{MODULE}._clone_has_dataset", lambda td, wanted: has_dataset)
    return SimpleNamespace(target_dir=target_dir, run_command=rc)


def _cmds(rc) -> list[list[str]]:
    return [c.args[0] for c in rc.call_args_list]


def test_existing_clone_with_dataset_is_reused(monkeypatch, tmp_path):
    env = _setup(monkeypatch, tmp_path, has_dataset=True)
    (env.target_dir / ".git").mkdir(parents=True)

    clone_dataset(DATASET)

    assert not any(cmd[:2] == ["git", "clone"] for cmd in _cmds(env.run_command))  # reused, no re-clone
    assert (env.target_dir / ".cloned").exists()


def test_existing_clone_without_dataset_is_recloned(monkeypatch, tmp_path):
    env = _setup(monkeypatch, tmp_path, has_dataset=False)
    (env.target_dir / ".git").mkdir(parents=True)

    clone_dataset(DATASET)

    assert any(cmd[:2] == ["git", "clone"] for cmd in _cmds(env.run_command))  # wrong repo --> re-cloned
    assert (env.target_dir / ".cloned").exists()
