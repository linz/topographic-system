import json
from pathlib import Path
from unittest.mock import MagicMock

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
    monkeypatch.setattr(working_data, "WORKING_DIR", tmp_path)
    monkeypatch.setattr(working_data, "WORKING_EXPORTS_DIR", export_dir)
    monkeypatch.setattr(working_data, "WORKING_LIFECYCLE_DIR", tmp_path / "lifecycle")
    monkeypatch.setattr(working_data, "env_working_data_s3_url", lambda: S3_URL)
    releases_file = export_dir / "nz_airport_polygons" / ".releases.json"
    releases_file.parent.mkdir(parents=True)
    releases_file.write_text(json.dumps({"66": {"commit": "abc", "location": "x"}}))
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

    summary = json.loads((tmp_path / "summary.json").read_text())
    assert summary == {"datasets": {"nz_airport_polygons": {"66": {"commit": "abc", "location": "x"}}}, "lookups": {}}


def _download_env(tmp_path, monkeypatch):
    source_dir = tmp_path / "source"
    monkeypatch.setattr(working_data, "WORKING_DIR", tmp_path)
    monkeypatch.setattr(working_data, "SOURCE_DIR", source_dir)
    monkeypatch.setattr(working_data, "WORKING_EXPORTS_DIR", tmp_path / "export")
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
