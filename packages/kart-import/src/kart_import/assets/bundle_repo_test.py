import subprocess

import pytest

from .bundle_repo import bundle_repo

MODULE = "kart_import.assets.bundle_repo"
REPO = "topographic-data"
# A fixed identity, so the test doesn't depend on the machine's git config
GIT_IDENTITY = ["-c", "user.name=test", "-c", "user.email=test@example.com", "-c", "commit.gpgsign=false"]


def _git(*args, cwd):
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=True).stdout.strip()


@pytest.fixture
def dirs(tmp_path, monkeypatch):
    output_dir = tmp_path / "output"
    bundle_dir = tmp_path / "bundle"
    monkeypatch.setattr(f"{MODULE}.OUTPUT_DIR", output_dir)
    monkeypatch.setattr(f"{MODULE}.BUNDLE_DIR", bundle_dir)
    return output_dir, bundle_dir


def _built_repo(output_dir):
    """A target repo with two commits on master, as `kart_import_repo` would leave it."""
    repo_dir = output_dir / REPO
    repo_dir.mkdir(parents=True)
    _git("init", "-b", "master", ".", cwd=repo_dir)
    for message in ("feat(airport): import release 64", "release: 64"):
        _git(*GIT_IDENTITY, "commit", "--allow-empty", "-m", message, cwd=repo_dir)
    return repo_dir


def test_bundle_holds_the_whole_history(dirs, tmp_path):
    output_dir, bundle_dir = dirs
    repo_dir = _built_repo(output_dir)

    bundle_file = bundle_repo(REPO)

    assert bundle_file == str((bundle_dir / f"{REPO}.bundle").resolve())
    clone_dir = tmp_path / "clone"
    _git("clone", bundle_file, str(clone_dir), cwd=tmp_path)
    assert _git("rev-parse", "HEAD", cwd=clone_dir) == _git("rev-parse", "HEAD", cwd=repo_dir)
    assert _git("rev-list", "--count", "HEAD", cwd=clone_dir) == "2"


def test_fails_when_repo_was_not_built(dirs):
    output_dir, bundle_dir = dirs
    output_dir.mkdir()

    # git can't run in a repo directory that doesn't exist
    with pytest.raises(FileNotFoundError):
        bundle_repo(REPO)
    assert not (bundle_dir / f"{REPO}.bundle").exists()
