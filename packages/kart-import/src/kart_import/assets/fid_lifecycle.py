import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from ..command import run_command
from ..config import SOURCE_DIR, WORKING_LIFECYCLE_DIR, Release, get_dataset_by_name, get_releases
from ..git.kart import get_kart_dataset_id
from ..git.release import get_release_commit
from ..log import log_context
from ..uuid7 import reproducable_uuid7_text

logger = logging.getLogger("kart_import")

# Git empty tree hash - used as a starting point for the first diff
EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"


def resolve_dataset_id(dataset_name: str, repo_dir: Path) -> str:
    """Dataset id within the (possibly multi-dataset) Kart repo.

    Prefer the explicit ``source.dataset`` from the theme config; fall back to
    auto-detecting the repo's sole dataset.
    """
    td = get_dataset_by_name(dataset_name)
    return td.source.dataset or get_kart_dataset_id(repo_dir)


def get_fid_lifecycle_file(dataset_name: str, releases: list[Release]) -> Path:
    """
    FID output lifecycle file location

    This varies based on which releases are expected
    """
    return WORKING_LIFECYCLE_DIR / f"{dataset_name}_release{releases[0].id}-{releases[-1].id}.json"


def get_mapping_commit(repo_dir: Path, dataset_id: str) -> tuple[str, datetime] | None:
    """Finds the first commit that introduced t50_fid in the schema."""

    schema_path = f"{dataset_id}/.table-dataset/meta/schema.json"

    # Optimization: Try March 2015 first as most mappings happened then
    cmd = [
        "git",
        "log",
        "-S",
        "t50_fid",
        "--since=2015-03-01",
        "--until=2015-04-01",
        "--date=iso",
        "--reverse",
        "--pretty=format:%H|%ad",
        "--",
        schema_path,
    ]
    stdout = run_command(cmd, cwd=str(repo_dir))
    commits = stdout.strip().split("\n")
    if commits and commits[0]:
        first_line = commits[0].split("|")
        return first_line[0], datetime.fromisoformat(first_line[1])

    # Fallback: Search entire history
    cmd = ["git", "log", "-S", "t50_fid", "--date=iso", "--reverse", "--pretty=format:%H|%ad", "--", schema_path]
    stdout = run_command(cmd, cwd=str(repo_dir)).strip()
    if not stdout:
        logger.info(f"No t50_fid mapping found in {schema_path}")
        return None
    result = stdout.split("\n")[0].split("|")
    return result[0], datetime.fromisoformat(result[1])


def make_lifecycle_id(commit_time: str, fid: str, fid_field: str, dataset_id: str) -> str:
    """Generate a reproducible UUID for a lifecycle entry.

    `t50_fid` is globally unique, so the fid alone seeds the UUID. Any other
    field (auto_pk, or a configured per-dataset key) is only unique within its
    dataset, so it is namespaced by the dataset id.
    """
    ts_ms = int(datetime.fromisoformat(commit_time).timestamp() * 1000)
    if fid_field != "t50_fid":
        return str(reproducable_uuid7_text(ts_ms, f"{dataset_id}:{fid}"))
    return str(reproducable_uuid7_text(ts_ms, fid))


def parse_kart_diff(
    stdout: str,
    lifecycle: dict[str, Any],
    commit_time: str,
    dataset_id: str,
    fid_field: str,
) -> None:
    for line in stdout.splitlines():
        if not line.strip():
            continue
        diff_entry = json.loads(line)
        if diff_entry.get("type") != "feature":
            continue

        change_obj = diff_entry.get("change", {}).get("++")

        if change_obj is None:
            continue

        fid = change_obj.get(fid_field)
        if fid is None:
            raise Exception(f"Change without fid: {line}")

        fid_str = str(fid)
        # Sometimes fids are joined in the reblocker then unjoined later, so skip if we've already seen this fid
        if fid_str in lifecycle:
            logger.info(f"skipping duplicate fid {fid_str} - previously seen: {lifecycle[fid_str]['created_at']}")
            continue

        lifecycle[fid_str] = {
            "id": make_lifecycle_id(commit_time, fid_str, fid_field, dataset_id),
            "created_at": commit_time,
        }


def generate_lifecycle(dataset_name: str):
    repo_dir = SOURCE_DIR / dataset_name
    td = get_dataset_by_name(dataset_name)
    dataset_id = resolve_dataset_id(dataset_name, repo_dir)
    releases = get_releases()

    lifecycle: dict[str, dict[str, str]] = {}

    # Walk releases and diff for updates
    last_commit = EMPTY_TREE

    # Determine which field to use as the feature identifier
    if td.fid_field:
        fid_field = td.fid_field
        logger.info(f"Using configured fid_field={fid_field} for {dataset_name}")
    elif get_mapping_commit(repo_dir, dataset_id):
        fid_field = "t50_fid"
        logger.info(f"Using t50_fid for {dataset_name}")
    else:
        fid_field = "auto_pk"
        logger.info(f"No t50_fid mapping for {dataset_name}, using auto_pk")

    for release in releases:
        res = get_release_commit(repo_dir, release.until)
        if not res:
            logger.debug(f"No commit for release {release.id}. Skipping.")
            continue

        commit, commit_time = res

        if commit == last_commit:
            logger.debug(f"Release {release.id} has same commit as previous. Skipping diff.")
            continue

        stdout = run_command(
            # Scope the diff to this dataset; the source repo may hold several
            ["kart", "diff", f"{last_commit}...{commit}", "-o", "json-lines", "--delta-filter=++", "--", dataset_id],
            cwd=str(repo_dir),
        )
        parse_kart_diff(stdout, lifecycle, commit_time, dataset_id, fid_field)

        last_commit = commit

    WORKING_LIFECYCLE_DIR.mkdir(parents=True, exist_ok=True)
    output_file = get_fid_lifecycle_file(dataset_name, releases)
    with open(output_file, "w") as f:
        json.dump(lifecycle, f, indent=2)
    logger.info("write lifecycle", extra={"fids": len(lifecycle), "target": output_file})

    return str(output_file)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m kart_import.assets.fid_lifecycle <dataset_name>")
        sys.exit(1)

    with log_context(action="export", dataset=sys.argv[1]):
        generate_lifecycle(sys.argv[1])
