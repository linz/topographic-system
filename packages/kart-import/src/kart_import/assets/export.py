import logging
import os
from dataclasses import dataclass

from ..command import run_command
from ..config import (
    SOURCE_DIR,
    WORKING_EXPORTS_DIR,
    get_releases,
    get_source_entry,
)
from ..git.kart import get_kart_dataset_id, is_kart
from ..git.release import get_release_commit
from ..log import log_context
from ..thread import run_in_thread_pool

logger = logging.getLogger("kart_import")


@dataclass
class CommitData:
    commit: str
    commit_time: str
    releases: list[int]


def export_dataset_releases(dataset_name: str):
    td = get_source_entry(dataset_name)

    releases = get_releases()

    repo_dir = SOURCE_DIR / dataset_name
    if not is_kart(repo_dir):
        raise Exception(f"Kart repo not found: {repo_dir}")
    kart_dataset_id = td.source.dataset or get_kart_dataset_id(repo_dir)

    commit_to_releases: dict[str, CommitData] = {}

    for release in releases:
        res = get_release_commit(repo_dir, release.until)
        if res is None:
            continue
        commit, commit_time = res
        if commit not in commit_to_releases:
            commit_to_releases[commit] = CommitData(
                commit=commit,
                commit_time=commit_time,
                releases=[],
            )
        commit_to_releases[commit].releases.append(release.id)

    def process_export_release(info: CommitData):
        target_commit = WORKING_EXPORTS_DIR / dataset_name
        target_commit.mkdir(parents=True, exist_ok=True)

        target_commit_file = target_commit / f"{info.commit_time[:10]}_{info.commit}.json"

        if not target_commit_file.exists():
            logger.info(
                f"Exporting {dataset_name} to GeoJSON",
                extra={"commit": info.commit, "releases": info.releases, "file": str(target_commit_file)},
            )

            cmd = ["kart", "export", "--overwrite", "--ref", info.commit, kart_dataset_id, str(target_commit_file)]
            run_command(cmd, cwd=str(repo_dir))

        for release_id in info.releases:
            release_output_dir = WORKING_EXPORTS_DIR / f"release_{release_id}"
            release_output_dir.mkdir(parents=True, exist_ok=True)
            release_output_file = release_output_dir / f"{dataset_name}.json"

            if release_output_file.exists():
                release_output_file.unlink()
            os.symlink(os.path.relpath(target_commit_file, release_output_dir), release_output_file)
            logger.info("link", extra={"release": release_id, "commit": commit, "commit_time": info.commit_time[0:10]})

    run_in_thread_pool(
        func=process_export_release,
        items=list(commit_to_releases.values()),
        thread_count=4,
    )

    representative_dir = WORKING_EXPORTS_DIR / f"release_{releases[-1].id}"
    return str(representative_dir / f"{dataset_name}.json")


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

    output_dir = WORKING_EXPORTS_DIR / "lookup" / lookup_name
    output_dir.mkdir(parents=True, exist_ok=True)

    for commit, commit_time in commits.items():
        output_file = output_dir / f"{commit}.json"
        if output_file.exists():
            continue
        logger.info(
            f"Exporting lookup {lookup_name} to GeoJSON",
            extra={"commit": commit, "commit_time": commit_time[:10], "file": str(output_file)},
        )
        cmd = ["kart", "export", "--overwrite", "--ref", commit, kart_dataset_id, str(output_file)]
        run_command(cmd, cwd=str(repo_dir))

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
