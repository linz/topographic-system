import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq

from ..command import run_command
from ..config import (
    EXPORT_FORMAT,
    SOURCE_DIR,
    get_dataset_by_name,
    get_releases,
    get_source_entry,
    get_source_repo_name,
)
from ..export_paths import (
    dataset_commit_file,
    dataset_empty_file,
    dataset_export_dir,
    dataset_release_file,
    export_filename,
    lookup_commit_file,
    lookup_dir,
    load_releases_map,
    relative_export_path,
    resolve_export_path,
    schema_sidecar_file,
    write_releases_map,
)
from ..git.kart import get_dataset_schema, get_kart_dataset_id, is_kart, ref_has_dataset
from ..git.release import get_release_commit
from ..log import log_context
from ..thread import run_in_thread_pool
from .fid_lifecycle import generate_lifecycle

logger = logging.getLogger("kart_import")


@dataclass
class CommitData:
    commit: str
    commit_time: str
    releases: list[int]


def gpd_read(path: Path) -> gpd.GeoDataFrame:
    """Read a GeoDataFrame from `path, Parquet files need a different reader than GeoJSON, and geopandas doesn't auto-detect by suffix."""
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
        gdf.to_parquet(path, index=False)
    else:
        gdf.to_file(path, driver="GeoJSON", index=False)


def link_release_export(source_file: Path, dataset_name: str, release_id: int) -> None:
    """Point a release's export at the (commit-named, possibly shared) file it resolves to."""
    release_output_file = dataset_release_file(dataset_name, release_id)
    release_output_dir = release_output_file.parent
    release_output_dir.mkdir(parents=True, exist_ok=True)

    # `is_symlink` as well as `exists`: a link left dangling by a deleted target reads as
    # non-existent, and `symlink` would then fail with EEXIST.
    if release_output_file.exists() or release_output_file.is_symlink():
        release_output_file.unlink()
    os.symlink(os.path.relpath(source_file, release_output_dir), release_output_file)


def export_empty_releases(dataset_name: str, release_ids: list[int]) -> None:
    """Give releases that predate the source repo's first commit an empty export.

    The `export` rule declares one output per release for every dataset, so a dataset whose repo
    starts partway through the release series (nz_trig_points: first commit 2019-07-09, after
    release 46's cutoff) would fail the rule with outputs that can never exist. An empty layer
    states the truth - the dataset had no features then - and lets transform and theme carry that
    emptiness through. Matches how `export_lookup` treats releases predating a lookup.

    One shared file for all such releases, symlinked like the commit exports.
    """
    if not release_ids:
        return

    empty_file = dataset_empty_file(dataset_name)
    empty_file.parent.mkdir(parents=True, exist_ok=True)
    if not empty_file.exists():
        write_export(gpd.GeoDataFrame({"geometry": gpd.GeoSeries([], crs="EPSG:4326")}), empty_file)

    for release_id in release_ids:
        link_release_export(empty_file, dataset_name, release_id)

    logger.info(
        "source predates these releases, exported empty",
        extra={"dataset": dataset_name, "releases": release_ids},
    )


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


def resolve_release_commits(dataset_name: str) -> tuple[dict[str, CommitData], list[int]]:
    """Each release's source commit, grouped by commit, plus the releases predating the dataset."""
    td = get_source_entry(dataset_name)

    repo_dir = SOURCE_DIR / get_source_repo_name(td)
    if not is_kart(repo_dir):
        raise Exception(f"Kart repo not found: {repo_dir}")
    kart_dataset_id = td.source.dataset or get_kart_dataset_id(repo_dir)

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
    """Releases of a `backfill` dataset that reuse its earliest export rather than their own."""
    if not get_dataset_by_name(dataset_name).backfill:
        return set()
    commit_to_releases, releases_before_first_commit = resolve_release_commits(dataset_name)
    return set(releases_before_first_commit) if commit_to_releases else set()


def export_dataset_releases(dataset_name: str):
    td = get_source_entry(dataset_name)
    releases = get_releases()
    repo_dir = SOURCE_DIR / get_source_repo_name(td)
    commit_to_releases, releases_before_first_commit = resolve_release_commits(dataset_name)
    kart_dataset_id = td.source.dataset or get_kart_dataset_id(repo_dir)
    if getattr(td, "backfill", False):
        releases_before_first_commit = backfill_releases(commit_to_releases, releases_before_first_commit)

    # {release_id: {"commit": sha | None, "location": <path relative to WORKING_EXPORTS_DIR>}},
    # persisted below so `relink_dataset_releases` can rebuild these symlinks from the downloaded
    # per-commit exports alone, with no clone involved. Threads below write disjoint keys (one
    # release id belongs to exactly one commit), so no lock is needed.
    releases_map: dict[int, dict[str, str | None]] = {}

    def export_release(info: CommitData):
        target_commit_file = dataset_commit_file(dataset_name, info.commit, info.commit_time)
        target_commit_file.parent.mkdir(parents=True, exist_ok=True)

        if not target_commit_file.exists():
            logger.info(
                f"Exporting {dataset_name} to {EXPORT_FORMAT}",
                extra={"commit": info.commit, "releases": info.releases, "file": str(target_commit_file)},
            )

            cmd = ["kart", "export", "--overwrite", "--ref", info.commit, kart_dataset_id, str(target_commit_file)]
            run_command(cmd, cwd=str(repo_dir))

        schema_file = schema_sidecar_file(target_commit_file)
        if not schema_file.exists():
            schema = get_dataset_schema(repo_dir, kart_dataset_id, info.commit)
            schema_file.write_text(json.dumps(schema))

        location = relative_export_path(target_commit_file)
        for release_id in info.releases:
            link_release_export(target_commit_file, dataset_name, release_id)
            releases_map[release_id] = {"commit": info.commit, "location": location}
            logger.info(
                "link", extra={"release": release_id, "commit": info.commit, "commit_time": info.commit_time[0:10]}
            )

    run_in_thread_pool(
        func=export_release,
        items=list(commit_to_releases.values()),
        thread_count=4,
    )

    export_empty_releases(dataset_name, releases_before_first_commit)
    if releases_before_first_commit:
        empty_location = relative_export_path(dataset_empty_file(dataset_name))
        for release_id in releases_before_first_commit:
            releases_map[release_id] = {"commit": None, "location": empty_location}

    write_releases_map(dataset_export_dir(dataset_name), releases_map)

    return str(dataset_release_file(dataset_name, releases[-1].id))


def relink_dataset_releases(dataset_name: str) -> None:
    """Rebuild a dataset's per-release export symlinks from its already-downloaded per-commit
    exports and persisted release map (see `write_releases_map` in `export_dataset_releases`).
    No clone needed -- used by `assets.working_data`'s S3 download, never by Snakemake directly.
    """
    releases_map = load_releases_map(dataset_export_dir(dataset_name))
    for release_id, info in releases_map.items():
        source_file = resolve_export_path(info["location"])
        link_release_export(source_file, dataset_name, release_id)


def export_dataset(dataset_name: str) -> str:
    """Export a dataset's lifecycle data and per-release files -- the two clone-only, stage-1-only
    steps that have no dependency on each other's output, merged into one entry point."""
    generate_lifecycle(dataset_name)
    return export_dataset_releases(dataset_name)


def export_lookup(lookup_name: str) -> str:
    """Export a lookup source once per commit across releases.

    Lookups are reference/attribute tables joined into datasets.
    Like datasets, each release resolves to the lookup commit as-of its cutoff.
    Releases sharing a commit share one export file (named by commit).
    Releases predating the lookup's first commit resolve to nothing and are simply not enriched.
    """
    lookup = get_source_entry(lookup_name)

    repo_dir = SOURCE_DIR / get_source_repo_name(lookup)
    if not is_kart(repo_dir):
        raise FileNotFoundError(f"lookup {lookup_name!r} is not a cloned kart repo (no .kart dir under {repo_dir})")
    kart_dataset_id = lookup.source.dataset or get_kart_dataset_id(repo_dir)

    commits: dict[str, str] = {}
    releases_map: dict[int, dict[str, str | None]] = {}
    for release in get_releases():
        res = get_release_commit(repo_dir, release.until)
        if res is None:
            releases_map[release.id] = {"commit": None, "location": None}
            continue
        commit, commit_time = res
        commits.setdefault(commit, commit_time)
        releases_map[release.id] = {"commit": commit, "location": None}  # location filled in below

    output_dir = lookup_dir(lookup_name)
    output_dir.mkdir(parents=True, exist_ok=True)

    def export_commit(commit: str) -> None:
        output_file = lookup_commit_file(lookup_name, commit)
        if not output_file.exists():
            logger.info(
                f"Exporting lookup {lookup_name} to {EXPORT_FORMAT}",
                extra={"commit": commit, "commit_time": commits[commit][:10], "file": str(output_file)},
            )
            cmd = ["kart", "export", "--overwrite", "--ref", commit, kart_dataset_id, str(output_file)]
            run_command(cmd, cwd=str(repo_dir))

        schema_file = schema_sidecar_file(output_file)
        if not schema_file.exists():
            schema = get_dataset_schema(repo_dir, kart_dataset_id, commit)
            schema_file.write_text(json.dumps(schema))

    run_in_thread_pool(func=export_commit, items=list(commits), thread_count=4)

    for info in releases_map.values():
        if info["commit"] is not None:
            info["location"] = relative_export_path(lookup_commit_file(lookup_name, info["commit"]))
    write_releases_map(output_dir, releases_map)

    # Prune exports for commits no longer resolved by any current release, so prepare_lookup doesn't keep slimming orphaned commits into stale parquets.
    referenced = {export_filename(commit) for commit in commits}
    for stale in output_dir.glob(export_filename("*")):
        if stale.name not in referenced:
            logger.info("pruning stale lookup export", extra={"lookup": lookup_name, "file": stale.name})
            stale.unlink()

    logger.info("export_lookup", extra={"lookup": lookup_name, "commits": len(commits)})
    return str(output_dir)


if __name__ == "__main__":
    import sys

    args = sys.argv[1:]
    if args and args[0] == "--lookup":
        if len(args) < 2:
            print("Usage: python -m kart_import.assets.export --lookup <lookup_name>")
            sys.exit(1)
        with log_context(action="export_lookup", lookup=args[1]):
            export_lookup(args[1])
    elif args:
        with log_context(action="export", dataset=args[0]):
            export_dataset(args[0])
    else:
        print("Usage: python -m kart_import.assets.export <dataset_name> | --lookup <lookup_name>")
        sys.exit(1)
