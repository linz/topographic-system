from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from .clone import clone_repo

REPO = "topographic-source-data"
MODULE = "kart_import.assets.clone"


def _setup(monkeypatch, tmp_path, *, has_dataset: bool):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    target_dir = source_dir / REPO

    def run_command(cmd, cwd=None, **kwargs):
        if cmd[:2] == ["git", "clone"]:
            Path(cmd[3]).mkdir(parents=True, exist_ok=True)  # real git clone creates the dir
        return ""

    rc = MagicMock(side_effect=run_command)
    monkeypatch.setattr(f"{MODULE}.SOURCE_DIR", source_dir)
    monkeypatch.setattr(f"{MODULE}.run_command", rc)
    monkeypatch.setattr(f"{MODULE}.env_use_bundle", lambda: False)  # exercise the direct-clone path
    monkeypatch.setattr(
        f"{MODULE}.get_source_repo",
        lambda name: [
            SimpleNamespace(source=SimpleNamespace(url="git@github.com:linz/topographic-source-data", dataset=ds))
            for ds in ("linz_map_sheet", "golf_sym")
        ],
    )
    monkeypatch.setattr(f"{MODULE}._clone_has_dataset", lambda td, wanted: has_dataset)
    return SimpleNamespace(target_dir=target_dir, run_command=rc)


def _cmds(rc) -> list[list[str]]:
    return [c.args[0] for c in rc.call_args_list]


def test_existing_clone_with_dataset_is_reused(monkeypatch, tmp_path):
    env = _setup(monkeypatch, tmp_path, has_dataset=True)
    (env.target_dir / ".git").mkdir(parents=True)

    clone_repo(REPO)

    assert not any(cmd[:2] == ["git", "clone"] for cmd in _cmds(env.run_command))  # reused, no re-clone
    assert (env.target_dir / ".cloned").exists()


def test_existing_clone_without_dataset_is_recloned(monkeypatch, tmp_path):
    env = _setup(monkeypatch, tmp_path, has_dataset=False)
    (env.target_dir / ".git").mkdir(parents=True)

    clone_repo(REPO)

    assert any(cmd[:2] == ["git", "clone"] for cmd in _cmds(env.run_command))  # wrong repo --> re-cloned
    assert (env.target_dir / ".cloned").exists()


def test_clone_must_contain_every_dataset_sourced_from_it(monkeypatch, tmp_path):
    """A shared clone is only reusable if it holds all of the datasets configured against it."""
    from . import clone

    monkeypatch.setattr(f"{MODULE}.source_ref", lambda repo_dir: "origin/master")
    monkeypatch.setattr(f"{MODULE}.ref_has_dataset", lambda repo_dir, ref, ds: ds == "linz_map_sheet")

    assert clone._clone_has_dataset(tmp_path, ["linz_map_sheet"])
    assert not clone._clone_has_dataset(tmp_path, ["linz_map_sheet", "golf_sym"])
    assert clone._clone_has_dataset(tmp_path, [])  # single-dataset koordinates repo


def test_bundle_clone_is_updated_from_the_real_source(monkeypatch, tmp_path):
    """Cloning seeds from the bundle for speed, then pulls from the real source since the
    published bundle may be stale."""
    source_dir = tmp_path / "source"
    source_dir.mkdir()

    def run_command(cmd, cwd=None, **kwargs):
        return ""

    def download_and_clone_from_bundle(bundle_target, repo_name, target_dir):
        target_dir.mkdir(parents=True, exist_ok=True)

    rc = MagicMock(side_effect=run_command)
    monkeypatch.setattr(f"{MODULE}.SOURCE_DIR", source_dir)
    monkeypatch.setattr(f"{MODULE}.run_command", rc)
    monkeypatch.setattr(f"{MODULE}.env_use_bundle", lambda: True)
    monkeypatch.setattr(f"{MODULE}.download_and_clone_from_bundle", download_and_clone_from_bundle)
    monkeypatch.setattr(
        f"{MODULE}.get_source_repo",
        lambda name: [
            SimpleNamespace(source=SimpleNamespace(url="git@github.com:linz/topographic-source-data", dataset=None))
        ],
    )
    monkeypatch.setattr(f"{MODULE}._clone_has_dataset", lambda td, wanted: True)

    clone_repo(REPO)

    cmds = [c.args[0] for c in rc.call_args_list]
    assert ["git", "remote", "set-url", "origin", "git@github.com:linz/topographic-source-data"] in cmds
    assert ["kart", "pull"] in cmds
    assert not any(cmd[:2] == ["git", "clone"] for cmd in cmds)


def test_bundle_clone_skips_source_pull_when_stubbed(monkeypatch, tmp_path):
    """KART_IMPORT_SKIP_SOURCE_PULL trusts the bundle as-is, for environments (e2e) with no
    credentials to reach the real source."""
    source_dir = tmp_path / "source"
    source_dir.mkdir()

    def run_command(cmd, cwd=None, **kwargs):
        return ""

    def download_and_clone_from_bundle(bundle_target, repo_name, target_dir):
        target_dir.mkdir(parents=True, exist_ok=True)

    rc = MagicMock(side_effect=run_command)
    monkeypatch.setattr(f"{MODULE}.SOURCE_DIR", source_dir)
    monkeypatch.setattr(f"{MODULE}.run_command", rc)
    monkeypatch.setattr(f"{MODULE}.env_use_bundle", lambda: True)
    monkeypatch.setattr(f"{MODULE}.env_skip_source_pull", lambda: True)
    monkeypatch.setattr(f"{MODULE}.download_and_clone_from_bundle", download_and_clone_from_bundle)
    monkeypatch.setattr(
        f"{MODULE}.get_source_repo",
        lambda name: [
            SimpleNamespace(source=SimpleNamespace(url="git@github.com:linz/topographic-source-data", dataset=None))
        ],
    )
    monkeypatch.setattr(f"{MODULE}._clone_has_dataset", lambda td, wanted: True)

    clone_repo(REPO)

    cmds = [c.args[0] for c in rc.call_args_list]
    assert not any(cmd[:2] == ["kart", "pull"] for cmd in cmds)
    assert not any(cmd[:2] == ["git", "clone"] for cmd in cmds)
