import logging
import shutil

from kart_import.log import log_context

from ..command import run_command
from ..config import OUTPUT_DIR, WORKING_THEME_DIR, get_releases

logger = logging.getLogger("kart_import")


def kart_import_theme(theme_name: str):
    repo_dir = OUTPUT_DIR / "theme" / theme_name
    bundle_file = OUTPUT_DIR / f"{theme_name}.bundle"

    releases = get_releases()

    # Init the Kart repo if it doesn't exist
    if (repo_dir / ".kart").exists():
        logger.info("Removing existing Kart Repo", extra={"target": str(repo_dir)})
        shutil.rmtree(repo_dir)

    repo_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Initializing Kart repo", extra={"target": str(repo_dir)})
    # -b master so the bundle's branch matches what kart_import_repo fetches
    run_command(["kart", "init", "-b", "master", "."], cwd=str(repo_dir))

    for release in releases:
        # Set commit date using standard Git environment variables
        env = {
            "GIT_AUTHOR_DATE": release.until.isoformat(),
            "GIT_COMMITTER_DATE": release.until.isoformat(),
        }

        input_file = WORKING_THEME_DIR / f"release_{release.id}" / f"{theme_name}.geojson"
        if not input_file.exists():
            logger.warning(f"Theme file not found: {input_file}. Skipping import.")
            continue

        logger.info("Importing release", extra={"release": release.id})

        cmd = [
            "kart",
            "import",
            "--message",
            f"import {theme_name} for release {release.id}",
            "--primary-key",
            "id",
            "--replace-existing",
            "--no-checkout",
            f"OGR:{input_file}",
        ]
        run_command(cmd, cwd=str(repo_dir), env=env, allow_error="No changes to commit")

    run_command(["kart", "git", "bundle", "create", str(bundle_file), "--all"], cwd=str(repo_dir))
    logger.info("All releases imported")
    return str(repo_dir)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m kart_import.assets.kart_import_theme <theme_name>")
        sys.exit(1)
    with log_context(action="kart_import_theme", theme=sys.argv[1]):
        kart_import_theme(sys.argv[1])
