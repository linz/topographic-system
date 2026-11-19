import json
import re

from . import export_paths


def test_source_commit_hash_is_a_git_sha():
    """Reads the pipeline's own HEAD, not the data being exported -- this repo is always a git
    checkout, so there's always a real answer."""
    assert re.fullmatch(r"[0-9a-f]{40}", export_paths.source_commit_hash())


def test_write_releases_map_includes_config_and_commit(tmp_path, monkeypatch):
    release_config = tmp_path / "topo50_release.yml"
    release_config.write_text("releases: []\n")
    monkeypatch.setattr(export_paths, "CONFIG_DIR_RELEASE", release_config)
    export_dir = tmp_path / "ds"
    export_dir.mkdir()

    export_paths.write_releases_map(export_dir, "ds", {66: {"commit": "abc", "location": "ds_abc.parquet"}})

    raw = json.loads(export_paths.releases_map_file(export_dir, "ds").read_text())
    assert raw["config"] == export_paths.release_config_hash()
    assert raw["commit"] == export_paths.source_commit_hash()
    assert raw["releases"] == {"66": {"commit": "abc", "location": "ds_abc.parquet"}}


def test_schema_sidecar_and_releases_map_have_no_leading_dot(tmp_path):
    export_file = tmp_path / "ds" / "ds_abc123.parquet"
    assert export_paths.schema_sidecar_file(export_file).name == "ds_abc123.parquet.schema.json"
    assert export_paths.releases_map_file(tmp_path / "ds", "ds").name == "ds.releases.json"


def test_entry_export_files_excludes_its_own_schema_sidecar_under_geojson(tmp_path, monkeypatch):
    """`EXPORT_SUFFIX=".json"` (KART_EXPORT_FORMAT=geojson) means a real export and its schema
    sidecar both end in `.json`, so a loose `<name>_*.json` glob would catch both -- without a
    leading dot to tell them apart, `entry_export_files` has to look at the payload shape instead.
    """
    monkeypatch.setattr(export_paths, "EXPORT_SUFFIX", ".json")
    export_dir = tmp_path / "topographic-source-data"
    export_dir.mkdir()

    real_export = export_dir / "road_lkp_abc123def456.json"
    real_export.write_text("{}")
    sidecar = export_paths.schema_sidecar_file(real_export)
    sidecar.write_text("[]")
    # A sibling entry sharing the same repo dir must never be picked up either.
    (export_dir / "other_entry_abc123def456.json").write_text("{}")

    found = export_paths.entry_export_files(export_dir, "road_lkp")

    assert found == [real_export]


def test_entry_export_files_matches_the_empty_placeholder(tmp_path, monkeypatch):
    monkeypatch.setattr(export_paths, "EXPORT_SUFFIX", ".parquet")
    export_dir = tmp_path / "nz_airport_polygons"
    export_dir.mkdir()
    empty_file = export_dir / "nz_airport_polygons_empty.parquet"
    empty_file.write_text("")

    assert export_paths.entry_export_files(export_dir, "nz_airport_polygons") == [empty_file]
