import json
import subprocess

import pytest

from . import kart


def _git(cwd, *args):
    subprocess.run(
        ["git", "-c", "user.email=t@e.st", "-c", "user.name=t", *args],
        cwd=str(cwd),
        check=True,
        capture_output=True,
    )


def _init_repo(path, filename="a.txt"):
    """A git repo with one commit and no remotes."""
    path.mkdir()
    _git(path, "init", "-b", "master")
    (path / filename).write_text("x")
    _git(path, "add", "-A")
    _git(path, "commit", "-m", "init")
    return path


def _origin_and_clone(tmp_path, dataset="linz_map_sheet"):
    """An origin holding a single top-level `dataset` tree, plus a clone of it.
    The clone gets the usual remote-tracking refs (origin/HEAD -> origin/master)."""
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


@pytest.mark.parametrize(
    "marker, expected",
    [
        (".kart", True),  # native kart repo
        (".git", True),  # plain git clone of a kart repo (KART_ALLOW_FROM_GIT)
        (None, False),  # neither -> not a repo
    ],
)
def test_is_kart(tmp_path, marker, expected):
    if marker:
        (tmp_path / marker).mkdir()
    assert kart.is_kart(tmp_path) is expected


@pytest.mark.parametrize(
    "drop_origin_head, expected",
    [
        (False, "origin/HEAD"),  # prefer the remote-tracking ref clone set up
        (True, "origin/master"),  # origin/HEAD gone -> fall through the chain, still a remote ref (not HEAD)
    ],
)
def test_source_ref_prefers_remote_tracking(tmp_path, drop_origin_head, expected):
    _, clone = _origin_and_clone(tmp_path)
    if drop_origin_head:
        _git(clone, "remote", "set-head", "origin", "--delete")
    assert kart.source_ref(clone) == expected


def test_source_ref_falls_back_to_head_without_remote(tmp_path):
    repo = _init_repo(tmp_path / "r")  # no remote -> no origin/* refs
    assert kart.source_ref(repo) == "HEAD"


@pytest.mark.parametrize(
    "ref, dataset, expected",
    [
        ("origin/master", "linz_map_sheet", True),  # present as a top-level tree
        ("origin/master", "not_a_dataset", False),  # absent
        ("origin/master", "linz_map", False),  # substring of a real name must NOT match
        ("no_such_ref", "linz_map_sheet", False),  # bad ref -> graceful False (check_error=False), no raise
    ],
)
def test_ref_has_dataset(tmp_path, ref, dataset, expected):
    _, clone = _origin_and_clone(tmp_path)
    assert kart.ref_has_dataset(clone, ref, dataset) is expected


SCHEMA = [
    {"name": "auto_pk", "dataType": "integer", "size": 64},
    {"name": "t50_fid", "dataType": "numeric", "precision": 10, "scale": 0},
]


def _dataset_repo(tmp_path, dataset="linz_road_cl", schema=SCHEMA):
    """A repo laid out the way kart repostructure v3 stores a dataset's schema."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "master")
    meta = repo / dataset / ".table-dataset" / "meta"
    meta.mkdir(parents=True)
    (meta / "schema.json").write_text(json.dumps(schema))
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "schema")
    return repo


def test_get_dataset_schema_reads_the_commit_not_a_checkout(tmp_path):
    """A blob in the commit: no working copy needed, and addressable by sha, so each exported
    commit is typed with the schema it actually had."""
    repo = _dataset_repo(tmp_path)
    sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=str(repo), check=True, capture_output=True, text=True
    ).stdout.strip()

    assert kart.get_dataset_schema(repo, "linz_road_cl", sha) == tuple(SCHEMA)
    assert kart.get_dataset_schema(repo, "linz_road_cl", "HEAD") == tuple(SCHEMA)  # refs work too


@pytest.mark.parametrize(
    "dataset, ref",
    [
        ("no_such_dataset", "HEAD"),  # dataset not in this repo (wrong clone)
        ("linz_road_cl", "no_such_ref"),  # commit not present (stale/shallow clone)
    ],
    ids=["missing-dataset", "missing-ref"],
)
def test_get_dataset_schema_raises_when_the_blob_is_absent(tmp_path, dataset, ref):
    """A missing schema must not read as 'nothing declared', silently skipping every restoration."""
    repo = _dataset_repo(tmp_path)
    with pytest.raises(FileNotFoundError, match="no kart schema at"):
        kart.get_dataset_schema(repo, dataset, ref)
