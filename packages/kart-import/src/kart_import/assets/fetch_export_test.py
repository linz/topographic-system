import io
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from .. import export_paths
from ..config import Source
from . import fetch_export

REPO_URL = "git@github.com:linz/topographic-source-data"


def _setup_entry(monkeypatch, tmp_path, *, entry_name="ds", releases=(66, 67), config_hash="cfg"):
    entry = SimpleNamespace(name=entry_name, source=Source(url=REPO_URL, dataset=None))
    monkeypatch.setattr(export_paths, "EXPORT_DIR", tmp_path)
    monkeypatch.setattr(export_paths, "get_source_entry", lambda _: entry)
    monkeypatch.setattr(fetch_export, "get_source_entry", lambda _: entry)
    monkeypatch.setattr(fetch_export, "get_releases", lambda: [SimpleNamespace(id=r) for r in releases])
    monkeypatch.setattr(fetch_export, "release_config_hash", lambda: config_hash)
    return export_paths.export_entry_dir(entry_name)


def _remote_releases_payload(releases: dict[int, dict], *, config_hash="cfg", commit_hash="sha") -> bytes:
    return json.dumps(
        {
            "config": config_hash,
            "commit": commit_hash,
            "dataset_id": "ds",
            "releases": {str(k): v for k, v in releases.items()},
        }
    ).encode()


def test_fetch_entry_https_downloads_only_missing_needed_releases(tmp_path, monkeypatch):
    """Fetches `<entry>.releases.json` over HTTPS, skips files already on disk, ignores releases
    outside `get_releases()`, and skips predating releases with `location: None`."""
    entry_dir = _setup_entry(monkeypatch, tmp_path, releases=(65, 66, 67))
    entry_dir.mkdir(parents=True)

    # Release 66's parquet is already cached locally; its sidecar and release 67's files are missing.
    (entry_dir / "ds_aaa.parquet").write_bytes(b"cached-aaa")

    remote_files = {
        "https://example.com/export/ds/ds.releases.json": _remote_releases_payload(
            {
                64: {"commit": "old", "commit_time": "2024-01-01T00:00:00Z", "location": "ds_old.parquet"},
                65: {"commit": None, "commit_time": None, "location": None},
                66: {"commit": "aaa", "commit_time": "2025-01-01T00:00:00Z", "location": "ds_aaa.parquet"},
                67: {"commit": "bbb", "commit_time": "2026-01-01T00:00:00Z", "location": "ds_bbb.parquet"},
            }
        ),
        "https://example.com/export/ds/ds_aaa.parquet.schema.json": b'[{"name": "t50_fid"}]',
        "https://example.com/export/ds/ds_bbb.parquet": b"remote-bbb",
        "https://example.com/export/ds/ds_bbb.parquet.schema.json": b'[{"name": "t50_fid"}]',
    }
    requested: list[str] = []

    def fake_urlopen(url, timeout=None):
        requested.append(url)
        return io.BytesIO(remote_files[url])

    monkeypatch.setattr(fetch_export.urllib.request, "urlopen", fake_urlopen)

    fetch_export.fetch_entry("ds", base_url="https://example.com/export")

    assert requested[0] == "https://example.com/export/ds/ds.releases.json"
    assert set(requested[1:]) == {
        "https://example.com/export/ds/ds_aaa.parquet.schema.json",
        "https://example.com/export/ds/ds_bbb.parquet",
        "https://example.com/export/ds/ds_bbb.parquet.schema.json",
    }
    assert (entry_dir / "ds_aaa.parquet").read_bytes() == b"cached-aaa"  # untouched
    assert (entry_dir / "ds_bbb.parquet").read_bytes() == b"remote-bbb"
    assert not (entry_dir / "ds_old.parquet").exists()  # release 64 not requested
    assert export_paths.entry_exported_sentinel("ds").exists()


def test_fetch_entry_s3_uses_aws_s3_cp(tmp_path, monkeypatch):
    entry_dir = _setup_entry(monkeypatch, tmp_path, releases=(66,))

    remote = {
        "s3://bucket/export/ds/ds.releases.json": _remote_releases_payload(
            {66: {"commit": "aaa", "commit_time": "2025-01-01T00:00:00Z", "location": "ds_aaa.parquet"}}
        ),
        "s3://bucket/export/ds/ds_aaa.parquet": b"parquet-bytes",
        "s3://bucket/export/ds/ds_aaa.parquet.schema.json": b"[]",
    }

    def fake_run(cmd, retries=0):
        assert cmd[:3] == ["aws", "s3", "cp"]
        Path(cmd[4]).write_bytes(remote[cmd[3]])
        return ""

    rc = MagicMock(side_effect=fake_run)
    monkeypatch.setattr(fetch_export, "run_command", rc)

    fetch_export.fetch_entry("ds", base_url="s3://bucket/export/")

    fetched_urls = {call.args[0][3] for call in rc.call_args_list}
    assert fetched_urls == set(remote)
    assert (entry_dir / "ds_aaa.parquet").read_bytes() == b"parquet-bytes"
    assert export_paths.entry_exported_sentinel("ds").exists()


def test_download_file_cleans_up_temp_file_on_error(tmp_path, monkeypatch):
    dest = tmp_path / "ds_aaa.parquet"

    def boom(url, timeout=None):
        raise OSError("network down")

    monkeypatch.setattr(fetch_export.urllib.request, "urlopen", boom)
    monkeypatch.setattr(fetch_export.time, "sleep", lambda _: None)

    with pytest.raises(OSError, match="network down"):
        fetch_export.download_file("https://example.com/ds_aaa.parquet", dest)

    assert not dest.exists()
    assert list(tmp_path.iterdir()) == []


def test_fetch_entry_rejects_mismatched_config_hash(tmp_path, monkeypatch):
    entry_dir = _setup_entry(monkeypatch, tmp_path, releases=(66,), config_hash="cfg")
    payload = _remote_releases_payload(
        {66: {"commit": "aaa", "commit_time": "2025-01-01T00:00:00Z", "location": "ds_aaa.parquet"}},
        config_hash="wrong-cfg",
    )
    monkeypatch.setattr(fetch_export.urllib.request, "urlopen", lambda url, timeout=None: io.BytesIO(payload))

    with pytest.raises(ValueError, match="config hash"):
        fetch_export.fetch_entry("ds", base_url="https://example.com/export")

    assert not export_paths.releases_map_file(entry_dir, "ds").exists()
    assert not export_paths.entry_exported_sentinel("ds").exists()


def test_fetch_entry_allows_different_pipeline_commit_hash(tmp_path, monkeypatch):
    """Stage 2 may run from a newer pipeline commit than Stage 1 as long as topo50_release.yml matches."""
    entry_dir = _setup_entry(monkeypatch, tmp_path, releases=(66,), config_hash="cfg")
    remote_files = {
        "https://example.com/export/ds/ds.releases.json": _remote_releases_payload(
            {66: {"commit": "aaa", "commit_time": "2025-01-01T00:00:00Z", "location": "ds_aaa.parquet"}},
            config_hash="cfg",
            commit_hash="older-pipeline-commit",
        ),
        "https://example.com/export/ds/ds_aaa.parquet": b"parquet",
        "https://example.com/export/ds/ds_aaa.parquet.schema.json": b"[]",
    }
    monkeypatch.setattr(fetch_export.urllib.request, "urlopen", lambda url, timeout=None: io.BytesIO(remote_files[url]))

    fetch_export.fetch_entry("ds", base_url="https://example.com/export")

    assert (entry_dir / "ds_aaa.parquet").read_bytes() == b"parquet"
    assert export_paths.entry_exported_sentinel("ds").exists()
