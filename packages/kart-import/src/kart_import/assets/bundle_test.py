from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from .bundle import bundle_dataset, fetch_bundle_head

SHA = "a" * 40  # up-to-date (bundle == remote)
SHA_OLD = "b" * 40  # stale bundle SHA
SHA_NEW = "c" * 40  # newer remote SHA
DATASET = "test_dataset"
S3_URL = "s3://test-bucket/source/"
MODULE = "kart_import.assets.bundle"


def _mock_urlopen(monkeypatch, payload: bytes):
    response = MagicMock()
    response.read.return_value = payload
    urlopen = MagicMock()
    urlopen.return_value.__enter__.return_value = response
    monkeypatch.setattr("urllib.request.urlopen", urlopen)


def test_fetch_bundle_head_valid(monkeypatch):
    _mock_urlopen(
        monkeypatch,
        b"# v2 git bundle\n"
        b"-894835e99fbe5c7df3280d5f8425c09ba6c2a801 prerequisite message\n"
        b"894835e99fbe5c7df3280d5f8425c09ba6c2a801 HEAD\n"
        b"1234567890abcdef1234567890abcdef12345678 refs/heads/main\n"
        b"\nPACK\x00\x00\x00...",
    )
    assert fetch_bundle_head("ds") == "894835e99fbe5c7df3280d5f8425c09ba6c2a801"


@pytest.mark.parametrize(
    "payload",
    [
        b"# v2 git bundle\n1234567890abcdef1234567890abcdef12345678 refs/heads/main\n\n",
        b"PACK\x00\x00\x00...",
    ],
    ids=["no_head_ref", "binary_garbage"],
)
def test_fetch_bundle_head_raises_without_head(monkeypatch, payload):
    _mock_urlopen(monkeypatch, payload)
    with pytest.raises(Exception, match="No HEAD found in bundle refs"):
        fetch_bundle_head("ds")


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Patches all `bundle_dataset` collaborators and exposes them as attributes."""
    source_dir = tmp_path / "source"
    source_dir.mkdir()

    # run_command dispatcher — tests tweak `state` to change behaviour.
    state = SimpleNamespace(remote_sha=SHA, s3_result="", commits="")

    def run_command(cmd, cwd=None, **kwargs):
        if "ls-remote" in cmd:
            return f"{state.remote_sha}\tHEAD\n"
        if "rev-parse" in cmd:
            return f"{state.remote_sha}\n"
        if cmd[:3] == ["aws", "s3", "ls"]:
            return state.s3_result
        if "--pretty=format:%H" in cmd:
            return state.commits
        return ""

    mocks = SimpleNamespace(
        source_dir=source_dir,
        target_dir=source_dir / DATASET,
        bundle_path=source_dir / f"{DATASET}.bundle",
        state=state,
        run_command=MagicMock(side_effect=run_command),
        fetch_bundle_head=MagicMock(return_value=None),
        download_bundle=MagicMock(),
        clone_from_bundle=MagicMock(),
        run_in_thread_pool=MagicMock(return_value=[]),
        should_pull=MagicMock(return_value=True),
    )

    monkeypatch.setattr(f"{MODULE}.SOURCE_DIR", source_dir)
    monkeypatch.setattr(f"{MODULE}.WORKING_EXPORTS_DIR", tmp_path / "exports")
    monkeypatch.setattr(f"{MODULE}.DATASET_MAP", {DATASET: MagicMock(source="kart@example.com:linz/test")})
    monkeypatch.setattr(f"{MODULE}.get_kart_dataset_id", MagicMock(return_value="ds_id"))
    monkeypatch.setattr(f"{MODULE}.env_bundle_s3_url", MagicMock(return_value=S3_URL))
    for name in (
        "run_command",
        "fetch_bundle_head",
        "download_bundle",
        "clone_from_bundle",
        "run_in_thread_pool",
        "should_pull",
    ):
        monkeypatch.setattr(f"{MODULE}.{name}", getattr(mocks, name))

    return mocks


def _cmds(env) -> list[list[str]]:
    return [c.args[0] for c in env.run_command.call_args_list]


def test_skip_when_bundle_matches_remote(env):
    env.fetch_bundle_head.return_value = SHA
    env.target_dir.mkdir()

    bundle_dataset(DATASET)

    assert (env.target_dir / ".bundle_created").exists()
    assert not any("create" in cmd for cmd in _cmds(env))
    env.run_in_thread_pool.assert_not_called()


def test_stale_downloads_and_clones_when_no_local_repo(env):
    env.fetch_bundle_head.return_value = SHA_OLD
    env.state.remote_sha = SHA_NEW
    env.target_dir.mkdir()

    bundle_dataset(DATASET)

    env.download_bundle.assert_called_once_with(DATASET, env.bundle_path)
    env.clone_from_bundle.assert_called_once()


def test_stale_skips_clone_when_local_repo_exists(env):
    env.fetch_bundle_head.return_value = SHA_OLD
    env.state.remote_sha = SHA_NEW
    (env.target_dir / ".kart").mkdir(parents=True)

    bundle_dataset(DATASET)

    env.download_bundle.assert_called_once()
    env.clone_from_bundle.assert_not_called()


def test_no_bundle_kart_clones_when_no_local_repo(env):
    env.target_dir.mkdir()

    bundle_dataset(DATASET)

    assert any(cmd[:2] == ["kart", "clone"] for cmd in _cmds(env))


@pytest.mark.parametrize("should_pull,expect_pull", [(True, True), (False, False)])
def test_no_bundle_pull_behavior_with_local_repo(env, should_pull, expect_pull):
    env.should_pull.return_value = should_pull
    (env.target_dir / ".kart").mkdir(parents=True)

    bundle_dataset(DATASET)

    cmds = _cmds(env)
    assert (["git", "pull"] in cmds) is expect_pull
    assert not any(cmd[:2] == ["kart", "clone"] for cmd in cmds)


def test_creates_bundle_and_uploads_to_s3(env):
    env.target_dir.mkdir()

    bundle_dataset(DATASET)

    cmds = _cmds(env)
    bundle_path = str(env.bundle_path)
    assert any("bundle" in cmd and "create" in cmd for cmd in cmds)
    assert any(cmd[:3] == ["aws", "s3", "cp"] and bundle_path in cmd for cmd in cmds)
    assert (env.target_dir / ".bundle_created").exists()


def _inline_pool(func, items, thread_count=4):
    return [func(item) for item in items]


@pytest.mark.parametrize(
    "s3_result,expect_export",
    [
        ("2024-01-01 00:00:00   1234 file.json.gz", False),  # already in S3 → skip
        ("", True),  # absent → export + upload
    ],
    ids=["already_in_s3", "missing_from_s3"],
)
def test_export_commit_behavior(env, s3_result, expect_export):
    commit_sha = "d" * 40
    env.state.commits = commit_sha
    env.state.s3_result = s3_result
    env.run_in_thread_pool.side_effect = _inline_pool
    env.target_dir.mkdir()

    bundle_dataset(DATASET)

    cmds = _cmds(env)
    exported = any(cmd[:2] == ["kart", "export"] and commit_sha in cmd for cmd in cmds)
    gzipped = any(cmd[0] == "gzip" for cmd in cmds)
    uploaded = any(cmd[:3] == ["aws", "s3", "cp"] and commit_sha in str(cmd) for cmd in cmds)

    assert exported is expect_export
    assert gzipped is expect_export
    assert uploaded is expect_export
