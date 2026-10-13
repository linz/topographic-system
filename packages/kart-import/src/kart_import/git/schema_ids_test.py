import json
import subprocess
from pathlib import Path

from .schema_ids import _stable_column_id, assign_stable_column_ids


def git(repo, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=str(repo), capture_output=True, text=True, check=True).stdout.strip()


def _commit_schema(repo: Path, theme: str, columns: list[dict]) -> str:
    """A minimal repo with just `<theme>/.table-dataset/meta/schema.json` committed, shaped like
    what a real `kart import` would produce for a schema-only stub."""
    schema_dir = repo / theme / ".table-dataset" / "meta"
    schema_dir.mkdir(parents=True, exist_ok=True)
    (schema_dir / "schema.json").write_text(json.dumps(columns))
    git(repo, "init", "-b", "master", ".")
    git(repo, "add", "-A")
    # A restricted, minimal env (no inherited $HOME) so a machine-wide `commit.gpgsign` config
    # can't be picked up and demand a passphrase this non-interactive test can't supply.
    subprocess.run(
        ["git", "commit", "-m", "schema stub"],
        cwd=str(repo),
        capture_output=True,
        check=True,
        env={
            "PATH": "/usr/bin:/bin",
            "GIT_AUTHOR_NAME": "Test",
            "GIT_AUTHOR_EMAIL": "test@example.com",
            "GIT_COMMITTER_NAME": "Test",
            "GIT_COMMITTER_EMAIL": "test@example.com",
        },
    )
    return git(repo, "rev-parse", "master")


def _schema(repo: Path, commit: str, theme: str) -> list[dict]:
    return json.loads(git(repo, "show", f"{commit}:{theme}/.table-dataset/meta/schema.json"))


def test_rewrites_only_the_id_field(tmp_path):
    columns = [
        {"id": "11111111-1111-1111-1111-111111111111", "name": "id", "dataType": "text", "primaryKeyIndex": 0},
        {"id": "22222222-2222-2222-2222-222222222222", "name": "value", "dataType": "text"},
    ]
    repo = tmp_path / "repo"
    commit = _commit_schema(repo, "airport", columns)

    new_commit = assign_stable_column_ids(repo, commit, "airport")

    assert _schema(repo, new_commit, "airport") == [
        {**columns[0], "id": _stable_column_id("airport", "id")},
        {**columns[1], "id": _stable_column_id("airport", "value")},
    ]


def test_deterministic_across_independently_minted_source_ids(tmp_path):
    """Two commits that differ only in which (e.g. randomly minted) id each column started with
    converge to the exact same rewritten schema.json -- the whole point of this module."""
    columns_a = [
        {"id": "aaaaaaaa-0000-0000-0000-000000000000", "name": "id", "dataType": "text", "primaryKeyIndex": 0},
        {"id": "aaaaaaaa-0000-0000-0000-000000000001", "name": "value", "dataType": "text"},
    ]
    columns_b = [
        {"id": "bbbbbbbb-0000-0000-0000-000000000000", "name": "id", "dataType": "text", "primaryKeyIndex": 0},
        {"id": "bbbbbbbb-0000-0000-0000-000000000001", "name": "value", "dataType": "text"},
    ]
    repo_a = tmp_path / "repo_a"
    repo_b = tmp_path / "repo_b"
    commit_a = _commit_schema(repo_a, "airport", columns_a)
    commit_b = _commit_schema(repo_b, "airport", columns_b)

    new_a = assign_stable_column_ids(repo_a, commit_a, "airport")
    new_b = assign_stable_column_ids(repo_b, commit_b, "airport")

    assert _schema(repo_a, new_a, "airport") == _schema(repo_b, new_b, "airport")


def test_different_theme_names_get_different_ids_for_the_same_column_name(tmp_path):
    """Two different themes both happen to have a column named `value` -- their stable ids must
    not collide, since they're unrelated datasets."""
    columns = [{"id": "11111111-1111-1111-1111-111111111111", "name": "value", "dataType": "text"}]
    repo_a = tmp_path / "repo_a"
    repo_b = tmp_path / "repo_b"
    commit_a = _commit_schema(repo_a, "airport", columns)
    commit_b = _commit_schema(repo_b, "island", columns)

    new_a = assign_stable_column_ids(repo_a, commit_a, "airport")
    new_b = assign_stable_column_ids(repo_b, commit_b, "island")

    assert _schema(repo_a, new_a, "airport")[0]["id"] != _schema(repo_b, new_b, "island")[0]["id"]


def test_leaves_the_rest_of_the_tree_untouched(tmp_path):
    columns = [{"id": "11111111-1111-1111-1111-111111111111", "name": "value", "dataType": "text"}]
    repo = tmp_path / "repo"
    (repo / "airport" / ".table-dataset" / "meta" / "crs").mkdir(parents=True)
    (repo / "airport" / ".table-dataset" / "meta" / "crs" / "EPSG:4326.wkt").write_text("GEOGCS[...]")
    commit = _commit_schema(repo, "airport", columns)
    original_crs_blob = git(repo, "rev-parse", f"{commit}:airport/.table-dataset/meta/crs/EPSG:4326.wkt")

    new_commit = assign_stable_column_ids(repo, commit, "airport")

    assert git(repo, "rev-parse", f"{new_commit}:airport/.table-dataset/meta/crs/EPSG:4326.wkt") == original_crs_blob
