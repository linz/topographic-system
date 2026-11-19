import hashlib
import json
from pathlib import Path
from unittest.mock import MagicMock

from .. import export_paths
from . import working_data

S3_URL = "s3://bucket/working/"


def _cp_writes_remote_summary(content: str):
    """A `run_command` side effect that mimics `aws s3 cp {s3}summary.json <dest>` writing
    `content` to its destination, and no-ops every other call."""

    def run_command(cmd, cwd=None, **kwargs):
        if cmd[:3] == ["aws", "s3", "cp"] and cmd[3] == f"{S3_URL}summary.json":
            Path(cmd[4]).write_text(content)
        return ""

    return run_command


def test_upload_working_data_syncs_then_publishes_a_summary(tmp_path, monkeypatch):
    """Touches no `.extracted*` sentinel itself -- `extract_all`/`extract_<repo>` each touch their
    own output (see the module docstring on `upload_working_data`)."""
    export_dir = tmp_path / "export"
    release_config = tmp_path / "topo50_release.yml"
    release_config.write_text("releases: []\n")
    monkeypatch.setattr(working_data, "WORKING_DIR", tmp_path)
    monkeypatch.setattr(working_data, "EXPORT_DIR", export_dir)
    monkeypatch.setattr(working_data, "WORKING_LIFECYCLE_DIR", tmp_path / "lifecycle")
    monkeypatch.setattr(export_paths, "CONFIG_DIR_RELEASE", release_config)
    monkeypatch.setattr(working_data, "env_working_data_s3_url", lambda: S3_URL)
    # One repo dir shared by two entries (a dataset and a lookup), each with its own named map.
    releases_dir = export_dir / "topographic-source-data"
    releases_dir.mkdir(parents=True)
    export_paths.write_releases_map(releases_dir, "nz_airport_polygons", {66: {"commit": "abc", "location": "x"}})
    run_command = MagicMock(return_value="")
    monkeypatch.setattr(working_data, "run_command", run_command)

    working_data.upload_working_data()

    calls = [c.args[0] for c in run_command.call_args_list]
    assert calls[:2] == [
        ["aws", "s3", "sync", str(export_dir), f"{S3_URL}export"],
        ["aws", "s3", "sync", str(tmp_path / "lifecycle"), f"{S3_URL}lifecycle"],
    ]
    assert calls[2][:4] == ["aws", "s3", "cp", str(tmp_path / "summary.json")]
    assert calls[2][4] == f"{S3_URL}summary.json"

    release_hash = hashlib.sha256(release_config.read_bytes()).hexdigest()
    commit_hash = export_paths.source_commit_hash()
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert summary == {
        "config": release_hash,
        "commit": commit_hash,
        "entries": {
            "nz_airport_polygons": {
                "config": release_hash,
                "commit": commit_hash,
                "releases": {"66": {"commit": "abc", "location": "x"}},
            }
        },
    }


def test_upload_working_data_skips_s3_when_disabled(tmp_path, monkeypatch):
    """KART_IMPORT_SKIP_UPLOAD leaves the run entirely local -- no `aws` calls at all -- while
    still writing summary.json locally."""
    export_dir = tmp_path / "export"
    release_config = tmp_path / "topo50_release.yml"
    release_config.write_text("releases: []\n")
    monkeypatch.setattr(working_data, "WORKING_DIR", tmp_path)
    monkeypatch.setattr(working_data, "EXPORT_DIR", export_dir)
    monkeypatch.setattr(working_data, "WORKING_LIFECYCLE_DIR", tmp_path / "lifecycle")
    monkeypatch.setattr(export_paths, "CONFIG_DIR_RELEASE", release_config)
    monkeypatch.setattr(working_data, "env_skip_upload", lambda: True)
    run_command = MagicMock(return_value="")
    monkeypatch.setattr(working_data, "run_command", run_command)

    working_data.upload_working_data()

    run_command.assert_not_called()
    assert (tmp_path / "summary.json").exists()


def test_upload_working_data_creates_export_dir_when_nothing_was_exported(tmp_path, monkeypatch):
    """`aws s3 sync` needs its local source to exist; nothing creates EXPORT_DIR at all if the
    requested KART_IMPORT_THEME/_RELEASE scope happened to resolve to zero datasets."""
    export_dir = tmp_path / "export"
    release_config = tmp_path / "topo50_release.yml"
    release_config.write_text("releases: []\n")
    monkeypatch.setattr(working_data, "WORKING_DIR", tmp_path)
    monkeypatch.setattr(working_data, "EXPORT_DIR", export_dir)
    monkeypatch.setattr(working_data, "WORKING_LIFECYCLE_DIR", tmp_path / "lifecycle")
    monkeypatch.setattr(export_paths, "CONFIG_DIR_RELEASE", release_config)
    monkeypatch.setattr(working_data, "env_working_data_s3_url", lambda: S3_URL)
    monkeypatch.setattr(working_data, "run_command", MagicMock(return_value=""))
    assert not export_dir.exists()

    working_data.upload_working_data()

    assert export_dir.is_dir()
    assert (tmp_path / "lifecycle").is_dir()


def _download_env(tmp_path, monkeypatch):
    source_dir = tmp_path / "source"
    monkeypatch.setattr(working_data, "WORKING_DIR", tmp_path)
    monkeypatch.setattr(working_data, "SOURCE_DIR", source_dir)
    monkeypatch.setattr(working_data, "EXPORT_DIR", tmp_path / "export")
    monkeypatch.setattr(working_data, "WORKING_LIFECYCLE_DIR", tmp_path / "lifecycle")
    monkeypatch.setattr(working_data, "SOURCE_REPO_MAP", {"topographic-source-data": []})
    monkeypatch.setattr(working_data, "DATASET_MAP", {"nz_airport_polygons": object()})
    monkeypatch.setattr(working_data, "env_working_data_s3_url", lambda: S3_URL)
    relinked = []
    monkeypatch.setattr(working_data, "relink_dataset_releases", lambda name: relinked.append(name))
    return source_dir, relinked


def test_download_working_data_syncs_when_the_summary_changed(tmp_path, monkeypatch):
    """No local summary yet (or it differs from S3's) -- sync for real, then cache the new
    summary, relink every dataset's release symlinks, and fake every source repo's `.cloned`."""
    source_dir, relinked = _download_env(tmp_path, monkeypatch)
    run_command = MagicMock(side_effect=_cp_writes_remote_summary('{"datasets": {}, "lookups": {}}'))
    monkeypatch.setattr(working_data, "run_command", run_command)

    working_data.download_working_data()

    calls = [c.args[0] for c in run_command.call_args_list]
    assert calls == [
        ["aws", "s3", "cp", f"{S3_URL}summary.json", str(tmp_path / ".remote_summary.json")],
        ["aws", "s3", "sync", f"{S3_URL}export", str(tmp_path / "export")],
        ["aws", "s3", "sync", f"{S3_URL}lifecycle", str(tmp_path / "lifecycle")],
    ]
    assert (tmp_path / "summary.json").read_text() == '{"datasets": {}, "lookups": {}}'
    assert relinked == ["nz_airport_polygons"]

    sentinel = source_dir / "topographic-source-data" / ".cloned"
    assert sentinel.exists()
    assert sentinel.stat().st_mtime == 0


def test_download_working_data_skips_sync_when_the_summary_is_unchanged(tmp_path, monkeypatch):
    """Local summary already matches what's in S3 -- skip the expensive sync entirely, but still
    relink and fake `.cloned` (cheap, and safe to redo even though nothing changed)."""
    source_dir, relinked = _download_env(tmp_path, monkeypatch)
    (tmp_path / "summary.json").write_text('{"datasets": {"ds": {}}, "lookups": {}}')
    run_command = MagicMock(side_effect=_cp_writes_remote_summary('{"datasets": {"ds": {}}, "lookups": {}}'))
    monkeypatch.setattr(working_data, "run_command", run_command)

    working_data.download_working_data()

    calls = [c.args[0] for c in run_command.call_args_list]
    assert calls == [["aws", "s3", "cp", f"{S3_URL}summary.json", str(tmp_path / ".remote_summary.json")]]
    assert relinked == ["nz_airport_polygons"]

    sentinel = source_dir / "topographic-source-data" / ".cloned"
    assert sentinel.exists()
    assert sentinel.stat().st_mtime == 0
