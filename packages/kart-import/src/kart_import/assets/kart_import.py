import shutil

from dagster import AssetExecutionContext, AssetKey, asset

from ..command import run_command
from ..config import OUTPUT_DIR, WORKING_THEME_DIR, get_releases, get_themes

# The Kart repository containing all themes
KART_REPO_DIR = OUTPUT_DIR / "kart"


@asset(
    name="kart_clean",
    group_name="kart_import",
)
def kart_import_clean(context: AssetExecutionContext):
    """Cleans the Kart repository directory to start from scratch."""
    if (KART_REPO_DIR / ".kart").exists():
        context.log.info(f"Cleaning existing Kart repo at {KART_REPO_DIR}")
        shutil.rmtree(KART_REPO_DIR)
    KART_REPO_DIR.mkdir(parents=True, exist_ok=True)
    return str(KART_REPO_DIR)


# Collect all theme assets for all releases to create a single bulk dependency list
all_releases = get_releases()
all_themes = [t for t in get_themes() if t.name != "all"]
bulk_deps = [AssetKey("kart_clean")]
for t in all_themes:
    bulk_deps.append(AssetKey(f"theme_{t.name}"))


@asset(name="kart_import", group_name="kart_import", deps=bulk_deps)
def bulk_kart_import(context: AssetExecutionContext):
    """
    Performs a bulk sequential import of all releases into Kart.
    This asset runs only after all theme aggregation for all releases is complete.
    """
    # Init the Kart repo if it doesn't exist
    if not (KART_REPO_DIR / ".kart").exists():
        KART_REPO_DIR.mkdir(parents=True, exist_ok=True)
        context.log.info(f"Initializing Kart repo at {KART_REPO_DIR}")
        run_command(context, ["kart", "init", "."], cwd=str(KART_REPO_DIR))

    for release in all_releases:
        # Set commit date using standard Git environment variables
        env = {
            "GIT_AUTHOR_DATE": release.date.isoformat(),
            "GIT_COMMITTER_DATE": release.date.isoformat(),
        }

        for theme in all_themes:
            input_file = WORKING_THEME_DIR / f"release_{release.id}" / f"{theme.name}.geojson"

            context.log.info(f"Importing {theme.name} for release {release.id} into Kart")

            # We run a single kart import for all themes in this release
            cmd = [
                "kart",
                "import",
                "--message",
                f"import {theme.name} for release {release.id}",
                "--primary-key",
                "t50_fid",
                "--replace-existing",
                "--allow-empty",
                f"OGR:{input_file}",
            ]

            print(cmd)
            run_command(context, cmd, cwd=str(KART_REPO_DIR), env=env)

        # Tag the release (use -f to allow overwriting during re-runs)
        tag_name = f"release_{release.id}"
        context.log.info(f"Tagging commit as {tag_name}")
        run_command(context, ["kart", "tag", "-f", tag_name], cwd=str(KART_REPO_DIR))

    context.log.info("All releases have been successfully imported and tagged in Kart.")
    return str(KART_REPO_DIR)


kart_import_assets = [kart_import_clean, bulk_kart_import]
