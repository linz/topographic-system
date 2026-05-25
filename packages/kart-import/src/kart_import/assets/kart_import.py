import shutil

from dagster import AssetExecutionContext, AssetKey, asset

from kart_import.config import get_kart_repos

from ..command import run_command
from ..config import OUTPUT_DIR, WORKING_THEME_DIR, Release, Theme, get_releases, get_themes


def kart_import_repo(context: AssetExecutionContext, repo_name: str, themes: list[Theme], releases: list[Release]):
    repo_dir = OUTPUT_DIR / repo_name

    # Init the Kart repo if it doesn't exist
    if (repo_dir / ".kart").exists():
        context.log.info(f"Removing existing Kart repo at {repo_dir}")
        shutil.rmtree(repo_dir)

    repo_dir.mkdir(parents=True, exist_ok=True)
    context.log.info(f"Initializing Kart repo at {repo_dir}")
    run_command(context, ["kart", "init", "."], cwd=str(repo_dir))

    for release in releases:
        # Set commit date using standard Git environment variables
        env = {
            "GIT_AUTHOR_DATE": release.until.isoformat(),
            "GIT_COMMITTER_DATE": release.until.isoformat(),
        }

        for theme in themes:
            input_file = WORKING_THEME_DIR / f"release_{release.id}" / f"{theme.name}.geojson"

            context.log.info(f"Importing {theme.name} for release {release.id} into Kart repo {repo_name}")

            cmd = [
                "kart",
                "import",
                "--message",
                f"import {theme.name} for release {release.id}",
                "--primary-key",
                "id",
                "--replace-existing",
                f"OGR:{input_file}",
            ]
            run_command(context, cmd, cwd=str(repo_dir), env=env, allow_error="No changes to commit")

        # Tag the release (use -f to allow overwriting during re-runs) in this repository
        tag_name = f"release_{release.id}"
        context.log.info(f"Tagging commit as {tag_name} in {repo_dir}")
        run_command(context, ["kart", "tag", "-f", tag_name], cwd=str(repo_dir))

    context.log.info(f"All releases have been successfully imported and tagged in Kart repo {repo_name}.")
    return str(repo_dir)


def make_kart_import_asset(repo_name: str, repo_themes: list[Theme], releases: list[Release]):
    deps = []
    for t in repo_themes:
        deps.append(AssetKey(f"theme_{t.name}"))

    @asset(
        name=f"kart_import_{repo_name}",
        group_name="kart_import",
        deps=deps,
    )
    def _kart_import_asset(context: AssetExecutionContext):
        return kart_import_repo(context, repo_name, repo_themes, releases)

    return _kart_import_asset


kart_import_assets = []
selected_release = get_releases()

for repo_name in get_kart_repos():
    repo_themes: list[Theme] = [t for t in get_themes() if t.target_repo == repo_name]
    kart_import_assets.append(make_kart_import_asset(repo_name, repo_themes, selected_release))
