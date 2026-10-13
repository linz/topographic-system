import subprocess

import pytest

from .linearise import TreeCollision, linearise


def git(repo, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=str(repo), capture_output=True, text=True, check=True).stdout.strip()


def make_theme_repo(
    tmp_path,
    theme: str,
    releases: list[tuple[str, str]],
    name: str | None = None,
    version: str = "3\n",
    release_ids: list[int] | None = None,
):
    """A repo shaped like a theme repo: one root subtree plus the shared version blob."""
    repo = tmp_path / f"theme_{name or theme}"
    (repo / theme / ".table-dataset").mkdir(parents=True)
    git(repo, "init", "-b", "master", ".")
    (repo / ".kart.repostructure.version").write_text(version)

    commits = []
    for index, (timestamp, content) in enumerate(releases):
        (repo / theme / ".table-dataset" / "feature").write_text(content)
        git(repo, "add", "-A")
        subject = (
            f"import {theme} for release {release_ids[index]}" if release_ids else f"import {theme} at {timestamp}"
        )
        subprocess.run(
            ["git", "commit", "-m", subject],
            cwd=str(repo),
            capture_output=True,
            check=True,
            env={
                "PATH": "/usr/bin:/bin",
                "GIT_AUTHOR_DATE": f"{timestamp} +0000",
                "GIT_COMMITTER_DATE": f"{timestamp} +0000",
                "GIT_AUTHOR_NAME": "Test",
                "GIT_AUTHOR_EMAIL": "test@example.com",
                "GIT_COMMITTER_NAME": "Test",
                "GIT_COMMITTER_EMAIL": "test@example.com",
            },
        )
        commits.append((timestamp, git(repo, "rev-parse", "HEAD")))
    return repo, commits


def combine(tmp_path, sources: dict[str, tuple], name: str = "combined") -> tuple:
    """Fetch every theme repo into one repo and return it with the ordered commit list."""
    combined = tmp_path / name
    combined.mkdir()
    git(combined, "init", "-b", "master", ".")

    dated: list[tuple[str, str]] = []
    for theme, (repo, commits) in sources.items():
        git(combined, "fetch", str(repo), f"master:{theme}")
        dated.extend((f"{timestamp}|{sha}", theme) for timestamp, sha in commits)

    return combined, [(theme, line.split("|")[1]) for line, theme in sorted(dated)]


def test_linearise_matches_cherry_pick(tmp_path):
    """The replayed history has the same trees, in the same order, as a cherry-pick."""
    sources = {
        "airport": make_theme_repo(tmp_path, "airport", [("1700000100", "a1"), ("1700000300", "a2")]),
        "island": make_theme_repo(tmp_path, "island", [("1700000200", "i1"), ("1700000400", "i2")]),
    }

    picked, commits = combine(tmp_path, sources, name="picked")
    git(picked, "config", "user.email", "test@example.com")
    git(picked, "config", "user.name", "Test")
    git(picked, "cherry-pick", *[sha for _, sha in commits])
    expected = git(picked, "log", "--reverse", "--format=%T %s", "master")

    replayed, commits = combine(tmp_path, sources, name="replayed")
    linearise(replayed, commits)

    assert git(replayed, "log", "--reverse", "--format=%T %s", "master") == expected
    # Interleaving is chronological, so the last commit holds both themes at their latest
    assert git(replayed, "show", "master:island/.table-dataset/feature") == "i2"
    assert git(replayed, "show", "master:airport/.table-dataset/feature") == "a2"


def test_linearise_preserves_committer_date(tmp_path):
    """Unlike cherry-pick, the replay keeps the source dates, so rebuilds are reproducible."""
    sources = {"airport": make_theme_repo(tmp_path, "airport", [("1700000100", "a1")])}
    repo, commits = combine(tmp_path, sources)
    linearise(repo, commits)

    assert git(repo, "log", "--format=%at %ct", "master") == "1700000100 1700000100"


def test_linearise_rejects_two_themes_writing_one_dataset(tmp_path):
    """An overlap cherry-pick would have flagged as a conflict must not silently win."""
    sources = {
        "airport": make_theme_repo(tmp_path, "airport", [("1700000100", "a1")]),
        "airport_dup": make_theme_repo(tmp_path, "airport", [("1700000200", "dup")], name="airport_dup"),
    }
    repo, commits = combine(tmp_path, sources)

    with pytest.raises(TreeCollision, match="both write the dataset 'airport'"):
        linearise(repo, commits)


def test_linearise_rejects_themes_disagreeing_on_a_shared_file(tmp_path):
    """Themes may both write a shared root-level blob, but only if they agree on its content."""
    sources = {
        "airport": make_theme_repo(tmp_path, "airport", [("1700000100", "a1")]),
        "island": make_theme_repo(tmp_path, "island", [("1700000200", "i1")], version="4\n"),
    }
    repo, commits = combine(tmp_path, sources)

    with pytest.raises(TreeCollision, match="disagree on shared file"):
        linearise(repo, commits)


def test_linearise_refuses_to_overwrite_an_existing_branch(tmp_path):
    """The first commit carries no `from`, so replaying onto a live branch would orphan it."""
    sources = {"airport": make_theme_repo(tmp_path, "airport", [("1700000100", "a1")])}
    repo, commits = combine(tmp_path, sources)
    linearise(repo, commits)
    before = git(repo, "rev-parse", "master")

    with pytest.raises(ValueError, match="already exists"):
        linearise(repo, commits)

    assert git(repo, "rev-parse", "master") == before


def _release_sources(tmp_path):
    return {
        "airport": make_theme_repo(
            tmp_path, "airport", [("1700000100", "a1"), ("1700000300", "a2")], release_ids=[30, 31]
        ),
        "island": make_theme_repo(
            tmp_path, "island", [("1700000200", "i1"), ("1700000400", "i2")], release_ids=[30, 31]
        ),
    }


def test_linearise_closes_each_release_with_a_marker(tmp_path):
    """Without it a release ends on whichever theme happened to sort last."""
    repo, commits = combine(tmp_path, _release_sources(tmp_path))

    linearise(repo, commits)

    assert git(repo, "log", "--reverse", "--format=%s", "master").splitlines() == [
        "import airport for release 30",
        "import island for release 30",
        "release 30",
        "import airport for release 31",
        "import island for release 31",
        "release 31",
    ]


def test_release_marker_carries_no_change(tmp_path):
    """Same tree as the commit it closes, so it costs a commit object and nothing else."""
    repo, commits = combine(tmp_path, _release_sources(tmp_path))

    linearise(repo, commits)

    assert git(repo, "rev-parse", "master^{tree}") == git(repo, "rev-parse", "master~1^{tree}")


def test_linearise_without_release_subjects_adds_no_markers(tmp_path):
    """The marker is keyed off the import subject; anything else replays unchanged."""
    sources = {"airport": make_theme_repo(tmp_path, "airport", [("1700000100", "a1")])}
    repo, commits = combine(tmp_path, sources)

    linearise(repo, commits)

    assert git(repo, "log", "--format=%s", "master").splitlines() == ["import airport at 1700000100"]
