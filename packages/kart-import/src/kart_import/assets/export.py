import os

from dagster import AssetExecutionContext, AssetKey, AssetsDefinition, asset

from ..command import run_command
from ..config import (
    SOURCE_DIR,
    WORKING_EXPORTS_DIR,
    Release,
    get_dataset_name,
    get_datasets,
    get_releases,
)
from ..git.kart import is_kart
from ..git.release import get_release_commit


def _export_dataset_release(ctx: AssetExecutionContext, dataset_source: str, releases: list[Release]):
    dataset_name = get_dataset_name(dataset_source)

    repo_dir = SOURCE_DIR / dataset_name
    if not is_kart(repo_dir):
        raise Exception(f"Kart repo not found: {repo_dir}")

    kart_dataset_id = run_command(ctx, ["kart", "data", "ls"], cwd=str(repo_dir)).strip()
    if "\n" in kart_dataset_id:
        raise Exception(f"Invalid dataset id: '{kart_dataset_id}'")

    last_commit = None
    for release in releases:
        res = get_release_commit(repo_dir, release.date)

        if not res:
            ctx.log.warning(f"No commit for {dataset_name} release {release.id} (before {release.date}). Skipping.")
            continue

        commit, commit_time = res

        output_dir = WORKING_EXPORTS_DIR / f"release_{release.id}"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{dataset_name}.json"

        if commit == last_commit:
            # Same commit as previous release — symlink instead of re-exporting
            previous_file = WORKING_EXPORTS_DIR / f"release_{release.id - 1}" / f"{dataset_name}.json"

            if output_file.exists() or output_file.is_symlink():
                output_file.unlink()
            os.symlink(previous_file, output_file)
            ctx.log.info(
                f"Linked {dataset_name} release {release.id} -> release {release.id - 1} (same commit {commit[:8]})"
            )
            continue

        last_commit = commit

        ctx.log.info(
            f"Exporting {dataset_name} for release {release.id} (commit {commit}) to GeoJSON: {output_file} - {commit_time}"
        )

        cmd = ["kart", "export", "--overwrite", "--ref", commit, kart_dataset_id, str(output_file)]
        run_command(ctx, cmd, cwd=str(repo_dir))

    return str(output_file)


def make_dataset_releases_asset(dataset_source: str, releases: list[Release]) -> AssetsDefinition:
    dataset_name = get_dataset_name(dataset_source)

    @asset(name=f"release_{dataset_name}", group_name="releases", deps=[AssetKey(f"clone_{dataset_name}")])
    def _releases_asset(context: AssetExecutionContext):
        return _export_dataset_release(context, dataset_source, releases)

    return _releases_asset


selected_releases = get_releases()
release_assets = [make_dataset_releases_asset(t, selected_releases) for t in get_datasets()]
