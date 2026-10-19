import os
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
            f"feat({theme}): import release {release_ids[index]}" if release_ids else f"import {theme} at {timestamp}"
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
        "feat(airport): import release 30",
        "feat(island): import release 30",
        "release: 30",
        "feat(airport): import release 31",
        "feat(island): import release 31",
        "release: 31",
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


def make_bucketed_theme_repo(
    tmp_path, theme: str, release_id: int, buckets: dict[str, str], timestamp: str = "1700000100"
):
    """A theme repo whose one commit writes several files under `<theme>/.table-dataset/feature/`,
    shaped like kart's own hash-bucketed feature layout, so a tiny `threshold` can split it."""
    repo = tmp_path / f"theme_{theme}_{release_id}"
    feature_dir = repo / theme / ".table-dataset" / "feature"
    feature_dir.mkdir(parents=True)
    git(repo, "init", "-b", "master", ".")
    (repo / ".kart.repostructure.version").write_text("3\n")
    for name, content in buckets.items():
        (feature_dir / name).write_text(content)
    git(repo, "add", "-A")
    subprocess.run(
        ["git", "commit", "-m", f"import {theme} for release {release_id}"],
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
    return repo, git(repo, "rev-parse", "HEAD")


def _incompressible(nbytes: int) -> str:
    """Content that won't shrink under pack compression, so its packed size stays predictable."""
    return os.urandom(nbytes).hex()


def test_linearise_splits_an_oversized_commit(tmp_path):
    """A commit whose new data exceeds `threshold` is replayed as several same-release commits."""
    buckets = {"00": _incompressible(200), "01": _incompressible(200), "02": _incompressible(200)}
    repo, sha = make_bucketed_theme_repo(tmp_path, "contour", 30, buckets)

    combined = tmp_path / "combined"
    combined.mkdir()
    git(combined, "init", "-b", "master", ".")
    git(combined, "fetch", str(repo), "master:contour")

    linearise(combined, [("contour", sha)], threshold=100)

    subjects = git(combined, "log", "--reverse", "--format=%s", "master").splitlines()
    assert subjects[-1] == "release 30"
    import_subjects = subjects[:-1]
    assert len(import_subjects) > 1, "the commit should actually have been split"
    for i, subject in enumerate(import_subjects, start=1):
        assert subject == f"import contour for release 30 (part {i}/{len(import_subjects)})"

    # the full tree still matches what one unsplit commit would have produced
    for name, content in buckets.items():
        assert git(combined, "show", f"master:contour/.table-dataset/feature/{name}") == content
    assert git(combined, "show", "master:.kart.repostructure.version") == "3"


def test_linearise_split_keeps_release_boundaries_correct(tmp_path):
    """Splitting a release's import must not disturb where the release marker lands."""
    buckets = {"00": _incompressible(200), "01": _incompressible(200)}
    repo30, sha30 = make_bucketed_theme_repo(tmp_path, "contour", 30, buckets, timestamp="1700000100")
    repo31, sha31 = make_bucketed_theme_repo(
        tmp_path, "contour", 31, {"00": "small change"}, timestamp="1700000200"
    )

    combined = tmp_path / "combined"
    combined.mkdir()
    git(combined, "init", "-b", "master", ".")
    git(combined, "fetch", str(repo30), "master:contour30")
    git(combined, "fetch", str(repo31), "master:contour31")

    linearise(combined, [("contour", sha30), ("contour", sha31)], threshold=100)

    subjects = git(combined, "log", "--reverse", "--format=%s", "master").splitlines()
    marker = subjects.index("release 30")
    assert marker > 0, "release 30 should have been split, not replayed as a single commit"
    assert all(s.startswith("import contour for release 30 (part ") for s in subjects[:marker])
    # the marker still closes release 30 exactly once, right before release 31's own (unsplit) import
    assert subjects[marker + 1 :] == ["import contour for release 31", "release 31"]


def test_linearise_does_not_split_under_threshold(tmp_path):
    """Small, everyday commits replay exactly as before -- unsplit, message untouched."""
    buckets = {"00": "tiny"}
    repo, sha = make_bucketed_theme_repo(tmp_path, "contour", 30, buckets)

    combined = tmp_path / "combined"
    combined.mkdir()
    git(combined, "init", "-b", "master", ".")
    git(combined, "fetch", str(repo), "master:contour")

    linearise(combined, [("contour", sha)], threshold=1_500_000_000)

    assert git(combined, "log", "--reverse", "--format=%s", "master").splitlines() == [
        "import contour for release 30",
        "release 30",
    ]
