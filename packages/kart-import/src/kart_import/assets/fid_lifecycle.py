import json
from datetime import datetime
from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext, AssetKey, AssetsDefinition, asset

from ..command import run_command
from ..config import SOURCE_DIR, WORKING_LIFECYCLE_DIR, ThemeDataset, get_dataset_name, get_releases, get_themes
from ..git.kart import kart_dataset_name
from ..git.release import get_release_commit
from ..uuid.uuid7 import reproducable_uuid7_text

# Git empty tree hash - used as a starting point for the first diff
EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"


def get_mapping_commit(context: AssetExecutionContext, repo_dir: Path) -> tuple[str, datetime] | None:
    """Finds the first commit that introduced t50_fid in the schema."""

    dataset_id = kart_dataset_name(context, repo_dir)
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
    stdout = run_command(context, cmd, cwd=str(repo_dir))
    commits = stdout.strip().split("\n")
    if commits and commits[0]:
        first_line = commits[0].split("|")
        return first_line[0], datetime.fromisoformat(first_line[1])

    # Fallback: Search entire history
    cmd = ["git", "log", "-S", "t50_fid", "--date=iso", "--reverse", "--pretty=format:%H|%ad", "--", schema_path]
    stdout = run_command(context, cmd, cwd=str(repo_dir)).strip()
    if not stdout:
        context.log.info(f"No t50_fid mapping found in {schema_path}")
        return None
    result = stdout.split("\n")[0].split("|")
    return result[0], datetime.fromisoformat(result[1])


def make_lifecycle_id(commit_time: str, fid: str, fid_field: str, dataset_id: str) -> str:
    """Generate a reproducible UUID for a lifecycle entry."""
    ts_ms = int(datetime.fromisoformat(commit_time).timestamp() * 1000)
    if fid_field == "auto_pk":
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

        change_obj = diff_entry.get("change", {})
        # Support both single '+' and double '++' representation of additions/modifications
        new_state = change_obj.get("+") or change_obj.get("++")
        old_state = change_obj.get("-") or change_obj.get("--")

        if new_state is None:
            continue

        fid = str(new_state.get(fid_field))
        if fid and fid != "None":
            if fid not in lifecycle:
                lifecycle[fid] = {
                    "id": make_lifecycle_id(commit_time, fid, fid_field, dataset_id),
                    "created_at": commit_time,
                    "updated_at": commit_time,
                }
            elif old_state:
                # Feature exists and was modified in this commit -> update updated_at
                lifecycle[fid]["updated_at"] = commit_time


def make_dataset_lifecycle_asset(dataset: ThemeDataset) -> AssetsDefinition:
    dataset_name = get_dataset_name(dataset.source)

    @asset(name=f"lifecycle_{dataset_name}", group_name="lifecycle", deps=[AssetKey(f"clone_{dataset_name}")])
    def _lifecycle_asset(context: AssetExecutionContext):
        dataset_name = get_dataset_name(dataset.source)
        repo_dir = SOURCE_DIR / dataset_name
        dataset_id = kart_dataset_name(context, repo_dir)
        releases = get_releases()

        lifecycle: dict[str, dict[str, str]] = {}

        # Walk releases and diff for updates
        last_commit = EMPTY_TREE

        # Determine which field to use as the feature identifier
        mapping_result = get_mapping_commit(context, repo_dir)
        if mapping_result:
            fid_field = "t50_fid"
            context.log.info(f"Using t50_fid for {dataset_name}")
        else:
            fid_field = "auto_pk"
            context.log.info(f"No t50_fid mapping for {dataset_name}, using auto_pk")

        for release in releases:
            res = get_release_commit(repo_dir, release.until)
            if not res:
                context.log.debug(f"No commit for release {release.id}. Skipping.")
                continue

            commit, commit_time = res

            if commit == last_commit:
                context.log.debug(f"Release {release.id} has same commit as previous. Skipping diff.")
                continue

            context.log.info(f"Diffing {dataset_name}: {last_commit} -> {commit} (Time {commit_time})")

            # We want both additions and modifications, so we omit --delta-filter=++
            cmd = ["kart", "diff", f"{last_commit}...{commit}", "-o", "json-lines"]

            stdout = run_command(context, cmd, cwd=str(repo_dir))
            parse_kart_diff(stdout, lifecycle, commit_time, dataset_id, fid_field)

            last_commit = commit

        # Save lifecycle for this dataset
        WORKING_LIFECYCLE_DIR.mkdir(parents=True, exist_ok=True)
        output_file = WORKING_LIFECYCLE_DIR / f"{dataset.name}.json"
        with open(output_file, "w") as f:
            json.dump(lifecycle, f, indent=2)

        return str(output_file)

    return _lifecycle_asset


# Generate assets
lifecycle_assets = []
seen_datasets = set()
for theme in get_themes():
    if theme.name == "all":
        continue
    for dataset in theme.datasets:
        if dataset.name not in seen_datasets:
            lifecycle_assets.append(make_dataset_lifecycle_asset(dataset))
            seen_datasets.add(dataset.name)


@asset(name="lifecycle_all", group_name="lifecycle", deps=[AssetKey(f"lifecycle_{name}") for name in seen_datasets])
def fid_lifecycle_master(context: AssetExecutionContext):
    pass
