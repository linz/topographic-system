import logging
import os
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd

from ..command import run_command
from ..config import EXPORT_FORMAT, SOURCE_DIR, get_releases, get_source_entry
from ..export_paths import (
    dataset_commit_file,
    dataset_empty_file,
    dataset_release_file,
    export_filename,
    lookup_commit_file,
    lookup_dir,
)
from ..git.kart import get_kart_dataset_id, is_kart, ref_has_dataset
from ..git.release import get_release_commit
from ..log import log_context
from ..thread import run_in_thread_pool

logger = logging.getLogger("kart_import")


@dataclass
class CommitData:
    commit: str
    commit_time: str
    releases: list[int]


def gpd_read(path: Path) -> gpd.GeoDataFrame:
    """Read a GeoDataFrame from `path, Parquet files need a different reader than GeoJSON, and geopandas doesn't auto-detect by suffix."""
    if path.suffix == ".parquet":
        return gpd.read_parquet(path)
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


def export_dataset_releases(dataset_name: str):
    td = get_source_entry(dataset_name)

    releases = get_releases()

    repo_dir = SOURCE_DIR / dataset_name
    if not is_kart(repo_dir):
        raise Exception(f"Kart repo not found: {repo_dir}")
    kart_dataset_id = td.source.dataset or get_kart_dataset_id(repo_dir)

    commit_to_releases: dict[str, CommitData] = {}
    releases_before_first_commit: list[int] = []

    for release in releases:
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

        for release_id in info.releases:
            link_release_export(target_commit_file, dataset_name, release_id)
            logger.info(
                "link", extra={"release": release_id, "commit": info.commit, "commit_time": info.commit_time[0:10]}
            )

    run_in_thread_pool(
        func=export_release,
        items=list(commit_to_releases.values()),
        thread_count=4,
    )

    export_empty_releases(dataset_name, releases_before_first_commit)

    return str(dataset_release_file(dataset_name, releases[-1].id))


def export_lookup(lookup_name: str) -> str:
    """Export a lookup source once per commit across releases.

    Lookups are reference/attribute tables joined into datasets.
    Like datasets, each release resolves to the lookup commit as-of its cutoff.
    Releases sharing a commit share one export file (named by commit).
    Releases predating the lookup's first commit resolve to nothing and are simply not enriched.
    """
    lookup = get_source_entry(lookup_name)

    repo_dir = SOURCE_DIR / lookup_name
    if not is_kart(repo_dir):
        raise FileNotFoundError(f"lookup {lookup_name!r} is not a cloned kart repo (no .kart dir under {repo_dir})")
    kart_dataset_id = lookup.source.dataset or get_kart_dataset_id(repo_dir)

    commits: dict[str, str] = {}
    for release in get_releases():
        res = get_release_commit(repo_dir, release.until)
        if res is not None:
            commit, commit_time = res
            commits.setdefault(commit, commit_time)

    output_dir = lookup_dir(lookup_name)
    output_dir.mkdir(parents=True, exist_ok=True)

    def export_commit(commit: str) -> None:
        output_file = lookup_commit_file(lookup_name, commit)
        if output_file.exists():
            return
        logger.info(
            f"Exporting lookup {lookup_name} to {EXPORT_FORMAT}",
            extra={"commit": commit, "commit_time": commits[commit][:10], "file": str(output_file)},
        )
        cmd = ["kart", "export", "--overwrite", "--ref", commit, kart_dataset_id, str(output_file)]
        run_command(cmd, cwd=str(repo_dir))

    run_in_thread_pool(func=export_commit, items=list(commits), thread_count=4)

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
            export_dataset_releases(args[0])
    else:
        print("Usage: python -m kart_import.assets.export <dataset_name> | --lookup <lookup_name>")
        sys.exit(1)
