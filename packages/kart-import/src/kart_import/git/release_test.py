import subprocess

from .release import get_release_commit


def _git(cwd, *args):
    return subprocess.run(
        ["git", "-c", "user.email=t@e.st", "-c", "user.name=t", *args],
        cwd=str(cwd),
        check=True,
        capture_output=True,
        text=True,
    )


def test_get_release_commit_uses_origin_tip_not_stale_local(tmp_path):
    """A stale / behind local branch must not shadow the fetched source tip: a bundle
    seed left local `master` at an old commit while `origin/master` advanced must return the origin tip."""
    origin = tmp_path / "origin"
    origin.mkdir()
    _git(origin, "init", "-b", "master")
    (origin / "a.txt").write_text("1")
    _git(origin, "add", "-A")
    _git(origin, "commit", "-m", "c1")

    clone = tmp_path / "clone"
    _git(tmp_path, "clone", str(origin), str(clone))

    # Advance origin, then fetch: origin/master is ahead while local HEAD stays at c1.
    (origin / "a.txt").write_text("2")
    _git(origin, "add", "-A")
    _git(origin, "commit", "-m", "c2")
    _git(clone, "fetch", "origin")

    origin_tip = _git(clone, "rev-parse", "origin/master").stdout.strip()
    local_head = _git(clone, "rev-parse", "HEAD").stdout.strip()
    assert origin_tip != local_head  # local is behind the fetched tip

    res = get_release_commit(clone, None)
    assert res is not None
    assert res[0] == origin_tip  # resolved against origin, not the stale local HEAD
