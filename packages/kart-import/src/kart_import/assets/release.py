from dagster import asset, AssetExecutionContext, AssetKey, AssetsDefinition
from ..config import (
    get_themes,
    RELEASES_DIR,
    SOURCE_DIR,
    get_releases,
    ThemeDataset,
    Release,
)
from ..command import run_command
from ..git.release import get_release_commit
from typing import Optional, List
import subprocess


def _export_dataset_release(
    ctx: AssetExecutionContext, dataset: ThemeDataset, releases: list[Release]
):
    repo_dir = SOURCE_DIR / dataset.source
    ctx.log.info(f"Checking for commit in {repo_dir} before {release.date}")


    kart_dataset_id = run_command(ctx, ["kart", "data", "ls"], cwd=str(repo_dir)).strip()
    if "\n" in kart_dataset_id:
        raise Exception(f"Invalid dataset id: '{kart_dataset_id}'")

    last_commit = None
    for release in releases:
        res = get_release_commit(repo_dir, release.date)
        commit, commit_time = res

        if not res:
            ctx.log.warning(f"No commit for {dataset.name} release {release.id} (before {release.date}). Skipping.")
            continue

        if commit = last_commit:
            ctx.log.warning(f"No commit for {dataset.name} release {release.id} (before {release.date}). Skipping.")
            continue

        last_commit = commit

        ctx.log.info(f"Exporting {dataset.name} for release {release.id} (commit {commit}) to GeoJSON: {output_file}")

        output_dir = RELEASES_DIR / f"release_{release.id}"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{dataset.name}.json"

        cmd = ["kart", "export", "--overwrite", "--ref", commit, kart_dataset_id, str(output_file)]
        run_command(ctx, cmd, cwd=str(repo_dir))

    return str(output_file)


def make_dataset_releases_asset(dataset: ThemeDataset, releases: List[Release]) -> AssetsDefinition:
    @asset(name=f"release_{dataset.name}", group_name="releases", deps=[AssetKey(f"clone_{dataset.name}")])
    def _releases_asset(context: AssetExecutionContext):
        return _export_dataset_release(context, dataset, releases)

    return _releases_asset


selected_releases = get_releases()
release_assets = [make_dataset_releases_asset(t, selected_releases) for t in get_themes()]
