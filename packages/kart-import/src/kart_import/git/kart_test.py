import subprocess

import pytest

from .kart import is_kart, ref_has_dataset, source_ref


def _git(cwd, *args):
    subprocess.run(
        ["git", "-c", "user.email=t@e.st", "-c", "user.name=t", *args], cwd=str(cwd), check=True, capture_output=True
    )


def _origin_and_clone(tmp_path, dataset="linz_map_sheet"):
    origin = tmp_path / "origin"
    origin.mkdir()
    _git(origin, "init", "-b", "master")
    (origin / dataset).mkdir()
    (origin / dataset / "f.txt").write_text("x")
    _git(origin, "add", "-A")
    _git(origin, "commit", "-m", "add dataset")
    clone = tmp_path / "clone"
    _git(tmp_path, "clone", str(origin), str(clone))
    return origin, clone


@pytest.mark.parametrize("layout", [".kart", ".git"])
def test_is_kart_accepts_both_layouts(tmp_path, layout):
    """Native kart repos (.kart) and plain git clones of kart repos (.git) both count."""
    (tmp_path / layout).mkdir()
    assert is_kart(tmp_path)


def test_is_kart_false_when_not_a_repo(tmp_path):
    assert not is_kart(tmp_path)


def test_source_ref_prefers_remote_tracking(tmp_path):
    _, clone = _origin_and_clone(tmp_path)
    assert source_ref(clone) == "origin/HEAD"


def test_source_ref_falls_back_to_head_without_remote(tmp_path):
    repo = tmp_path / "r"
    repo.mkdir()
    _git(repo, "init", "-b", "master")
    (repo / "a.txt").write_text("x")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "c")
    assert source_ref(repo) == "HEAD"


def test_ref_has_dataset(tmp_path):
    _, clone = _origin_and_clone(tmp_path, dataset="linz_map_sheet")
    assert ref_has_dataset(clone, "origin/master", "linz_map_sheet")
    assert not ref_has_dataset(clone, "origin/master", "not_a_dataset")
