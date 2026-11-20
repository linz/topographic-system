import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq

from ...command import run_command
from ...config import (
    EXPORT_FORMAT,
    SOURCE_DIR,
    get_dataset_by_name,
    get_releases,
    get_source_entry,
    get_source_repo_name,
)
from ...export_paths import (
    entry_commit_file,
    entry_export_filename,
    entry_export_files,
    entry_exported_sentinel,
    export_entry_dir,
    load_releases_map,
    schema_sidecar_file,
    write_releases_map,
)
from ...git.kart import get_dataset_schema, get_kart_dataset_id, is_kart, ref_has_dataset
from ...git.release import get_release_commit
from ...log import log_context
from ...thread import run_in_thread_pool

logger = logging.getLogger("kart_import")


@dataclass
class CommitData:
    commit: str
    commit_time: str
    releases: list[int]


def gpd_read(path: Path) -> gpd.GeoDataFrame:
    """Read a GeoDataFrame from `path`. Parquet files need a different reader than GeoJSON, and
    geopandas doesn't auto-detect by suffix."""
    if path.suffix == ".parquet":
        # OGR formats wrote a kart numeric as REAL; Parquet keeps it a decimal. Read it as a float, so
        # `kart_types.coerce_integer_columns` restores the declared integers as it does for OGR.
        gdf = gpd.read_parquet(path)
        decimals = [field.name for field in pq.read_schema(path) if pa.types.is_decimal(field.type)]
        return gdf.astype(dict.fromkeys(decimals, "float64"))
    return gpd.read_file(path, engine="pyogrio", use_arrow=True)


def write_export(gdf: gpd.GeoDataFrame, path: Path) -> None:
    """Write an export in the configured format (see EXPORT_FORMAT)."""
    if EXPORT_FORMAT == "parquet":
        gdf.to_parquet(path, compression="zstd", compression_level=9, index=False)
    else:
        gdf.to_file(path, driver="GeoJSON", index=False)


def backfill_releases(commit_to_releases: dict[str, CommitData], release_ids: list[int]) -> list[int]:
    """Hand the releases predating the source to its earliest commit, so they share its export.
    Returns the releases still left without data: all of them when there is no commit at all."""
    if not release_ids or not commit_to_releases:
        return release_ids
    earliest = min(commit_to_releases.values(), key=lambda c: datetime.fromisoformat(c.commit_time))
    earliest.releases[:0] = release_ids
    logger.info(
        "source predates these releases, backfilled from earliest commit",
        extra={"commit": earliest.commit, "releases": release_ids},
    )
    return []


def resolve_release_commits(entry_name: str) -> tuple[dict[str, CommitData], list[int]]:
    """Each release's source commit, grouped by commit, plus the releases predating the entry."""
    entry = get_source_entry(entry_name)

    repo_dir = SOURCE_DIR / get_source_repo_name(entry)
    if not is_kart(repo_dir):
        raise FileNotFoundError(f"Kart repo not found: {repo_dir}")
    kart_dataset_id = entry.source.dataset or get_kart_dataset_id(repo_dir)

    commit_to_releases: dict[str, CommitData] = {}
    releases_before_first_commit: list[int] = []

    for release in get_releases():
        res = get_release_commit(repo_dir, release.until)
        if res is None:
            releases_before_first_commit.append(release.id)
            continue
        commit, commit_time = res
        if not ref_has_dataset(repo_dir, commit, kart_dataset_id):
            releases_before_first_commit.append(release.id)
            continue
        if commit not in commit_to_releases:
            commit_to_releases[commit] = CommitData(
                commit=commit,
                commit_time=commit_time,
                releases=[],
            )
        commit_to_releases[commit].releases.append(release.id)

    return commit_to_releases, releases_before_first_commit


def backfilled_release_ids(dataset_name: str) -> set[int]:
    """Releases of a `backfill` dataset that reuse its earliest export rather than their own.
    Read from the persisted release map -- no source clone needed."""
    if not get_dataset_by_name(dataset_name).backfill:
        return set()
    releases_map = load_releases_map(export_entry_dir(dataset_name), dataset_name)
    return {release_id for release_id, info in releases_map.items() if info.get("backfilled")}


def export_entry(entry_name: str) -> str:
    """Export every distinct commit a dataset or lookup resolves to across configured releases,
    along with each commit's schema sidecar and `<entry_name>.releases.json`.

    Writes only under `EXPORT_DIR` (`data/export/<repo>/` + `.sentinel/<entry_name>.exported`).
    """
    entry = get_source_entry(entry_name)
    repo_name = get_source_repo_name(entry)
    repo_dir = SOURCE_DIR / repo_name
    commit_to_releases, releases_before_first_commit = resolve_release_commits(entry_name)
    kart_dataset_id = entry.source.dataset or get_kart_dataset_id(repo_dir)

    backfilled: set[int] = set()
    if getattr(entry, "backfill", False) and commit_to_releases:
        backfilled = set(releases_before_first_commit)
        releases_before_first_commit = backfill_releases(commit_to_releases, releases_before_first_commit)

    output_dir = export_entry_dir(entry_name)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Threads below write disjoint keys (one release id belongs to exactly one commit), so no lock is needed.
    releases_map: dict[int, dict[str, Any]] = {}

    def export_commit(info: CommitData) -> None:
        target_commit_file = entry_commit_file(entry_name, info.commit)

        if not target_commit_file.exists():
            logger.info(
                f"Exporting {entry_name} to {EXPORT_FORMAT}",
                extra={"commit": info.commit, "releases": info.releases, "file": str(target_commit_file)},
            )
            # Keep the original suffix so `kart export` detects the driver, and stay in the same
            # directory so `replace()` is an atomic same-filesystem rename.
            tmp_file = target_commit_file.with_name(f".tmp_{target_commit_file.name}")
            cmd = ["kart", "export", "--overwrite", "--ref", info.commit]
            if EXPORT_FORMAT == "parquet":
                cmd.extend(["-lco", "COMPRESSION=ZSTD", "-lco", "COMPRESSION_LEVEL=9"])
            cmd.extend([kart_dataset_id, str(tmp_file)])
            try:
                run_command(cmd, cwd=str(repo_dir))
                tmp_file.replace(target_commit_file)
            except BaseException:
                if tmp_file.exists():
                    tmp_file.unlink()
                raise

        schema_file = schema_sidecar_file(target_commit_file)
        if not schema_file.exists():
            schema = get_dataset_schema(repo_dir, kart_dataset_id, info.commit)
            tmp_schema = schema_file.with_name(f".tmp_{schema_file.name}")
            try:
                tmp_schema.write_text(json.dumps(schema))
                tmp_schema.replace(schema_file)
            except BaseException:
                if tmp_schema.exists():
                    tmp_schema.unlink()
                raise

        location = target_commit_file.name
        for release_id in info.releases:
            record: dict[str, Any] = {
                "commit": info.commit,
                "commit_time": info.commit_time,
                "location": location,
            }
            if release_id in backfilled:
                record["backfilled"] = True
            releases_map[release_id] = record

    run_in_thread_pool(
        func=export_commit,
        items=list(commit_to_releases.values()),
        thread_count=4,
    )

    if releases_before_first_commit:
        for release_id in releases_before_first_commit:
            releases_map[release_id] = {"commit": None, "commit_time": None, "location": None}
        logger.info(
            "source predates these releases, recorded empty",
            extra={"entry": entry_name, "releases": releases_before_first_commit},
        )

    write_releases_map(output_dir, entry_name, releases_map, dataset_id=kart_dataset_id)

    # Prune exports for commits no longer resolved by any current release, scoped to this entry's
    # own files (output_dir is shared with siblings from the same source repo).
    referenced = {entry_export_filename(entry_name, commit) for commit in commit_to_releases}
    for stale in entry_export_files(output_dir, entry_name):
        if stale.name not in referenced:
            logger.info("pruning stale export", extra={"entry": entry_name, "file": stale.name})
            stale.unlink()
            stale_schema = schema_sidecar_file(stale)
            if stale_schema.exists():
                stale_schema.unlink()

    sentinel = entry_exported_sentinel(entry_name)
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    sentinel.touch()

    logger.info("export", extra={"dataset": entry_name, "repo": repo_name, "commits": len(commit_to_releases)})
    return str(output_dir)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m kart_import.assets.extract.export <entry_name>")
        sys.exit(1)
    entry_name = sys.argv[1]
    repo_name = get_source_repo_name(get_source_entry(entry_name))
    with log_context(action="export", dataset=entry_name, repo=repo_name):
        export_entry(entry_name)
