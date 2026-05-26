import os
from dataclasses import dataclass

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
from ..git.kart import get_kart_dataset_id, is_kart
from ..git.release import get_release_commit
from ..thread import run_in_thread_pool


@dataclass
class CommitData:
    commit: str
    commit_time: str
    releases: list[int]
    first_release_id: int


def _export_dataset_release(ctx: AssetExecutionContext, dataset_source: str, releases: list[Release]):
    dataset_name = get_dataset_name(dataset_source)

    repo_dir = SOURCE_DIR / dataset_name
    if not is_kart(repo_dir):
        raise Exception(f"Kart repo not found: {repo_dir}")

    kart_dataset_id = get_kart_dataset_id(ctx, repo_dir)

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
                first_release_id=release.id,
            )
        commit_to_releases[commit].releases.append(release.id)

    def process_export_release(info: CommitData):
        first_release_id = info.first_release_id

        output_dir = WORKING_EXPORTS_DIR / f"release_{first_release_id}"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{dataset_name}.json"

        if output_file.exists() or output_file.is_symlink():
            output_file.unlink()

        ctx.log.info(
            f"Exporting {dataset_name} for release {first_release_id} (commit {info.commit}) to GeoJSON: {output_file} - {info.commit_time}"
        )

        cmd = ["kart", "export", "--overwrite", "--ref", info.commit, kart_dataset_id, str(output_file)]
        run_command(ctx, cmd, cwd=str(repo_dir))

        for release_id in info.releases[1:]:
            release_output_dir = WORKING_EXPORTS_DIR / f"release_{release_id}"
            release_output_dir.mkdir(parents=True, exist_ok=True)
            release_output_file = release_output_dir / f"{dataset_name}.json"

            if release_output_file.exists() or release_output_file.is_symlink():
                release_output_file.unlink()
            os.symlink(os.path.relpath(output_file, release_output_dir), release_output_file)

    run_in_thread_pool(
        context=ctx,
        func=process_export_release,
        items=list(commit_to_releases.values()),
        thread_count=4,
        description=f"Exporting {dataset_name} unique commits in parallel",
    )

    representative_dir = WORKING_EXPORTS_DIR / f"release_{releases[-1].id}"
    return str(representative_dir / f"{dataset_name}.json")


def make_dataset_releases_asset(dataset_source: str, releases: list[Release]) -> AssetsDefinition:
    dataset_name = get_dataset_name(dataset_source)

    @asset(name=f"release_{dataset_name}", group_name="releases", deps=[AssetKey(f"clone_{dataset_name}")])
    def _releases_asset(context: AssetExecutionContext):
        return _export_dataset_release(context, dataset_source, releases)

    return _releases_asset


selected_releases = get_releases()
release_assets = [make_dataset_releases_asset(t, selected_releases) for t in get_datasets()]
