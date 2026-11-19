from unittest.mock import MagicMock

from . import working_data


def test_upload_working_data_syncs_export_and_lifecycle(tmp_path, monkeypatch):
    """Touches no sentinel itself -- `extract_all`/`extract_<repo>` each touch their own output
    (see the module docstring on `upload_working_data`)."""
    monkeypatch.setattr(working_data, "WORKING_EXPORTS_DIR", tmp_path / "export")
    monkeypatch.setattr(working_data, "WORKING_LIFECYCLE_DIR", tmp_path / "lifecycle")
    monkeypatch.setattr(working_data, "env_working_data_s3_url", lambda: "s3://bucket/working/")
    run_command = MagicMock(return_value="")
    monkeypatch.setattr(working_data, "run_command", run_command)

    working_data.upload_working_data()

    calls = [c.args[0] for c in run_command.call_args_list]
    assert calls == [
        ["aws", "s3", "sync", str(tmp_path / "export"), "s3://bucket/working/export"],
        ["aws", "s3", "sync", str(tmp_path / "lifecycle"), "s3://bucket/working/lifecycle"],
    ]


def test_download_working_data_syncs_then_relinks_and_fakes_the_clone_sentinel(tmp_path, monkeypatch):
    """After the sync, every dataset gets its release symlinks rebuilt (no clone involved), and
    every source repo gets an ancient `.cloned` sentinel so Snakemake doesn't try to re-clone it
    just because the real clone was never made here (see `assets.working_data` module docstring).
    """
    source_dir = tmp_path / "source"
    monkeypatch.setattr(working_data, "SOURCE_DIR", source_dir)
    monkeypatch.setattr(working_data, "WORKING_EXPORTS_DIR", tmp_path / "export")
    monkeypatch.setattr(working_data, "WORKING_LIFECYCLE_DIR", tmp_path / "lifecycle")
    monkeypatch.setattr(working_data, "SOURCE_REPO_MAP", {"topographic-source-data": []})
    monkeypatch.setattr(working_data, "DATASET_MAP", {"nz_airport_polygons": object()})
    monkeypatch.setattr(working_data, "env_working_data_s3_url", lambda: "s3://bucket/working/")
    run_command = MagicMock(return_value="")
    monkeypatch.setattr(working_data, "run_command", run_command)
    relinked = []
    monkeypatch.setattr(working_data, "relink_dataset_releases", lambda name: relinked.append(name))

    working_data.download_working_data()

    calls = [c.args[0] for c in run_command.call_args_list]
    assert calls == [
        ["aws", "s3", "sync", "s3://bucket/working/export", str(tmp_path / "export")],
        ["aws", "s3", "sync", "s3://bucket/working/lifecycle", str(tmp_path / "lifecycle")],
    ]
    assert relinked == ["nz_airport_polygons"]

    sentinel = source_dir / "topographic-source-data" / ".cloned"
    assert sentinel.exists()
    assert sentinel.stat().st_mtime == 0
