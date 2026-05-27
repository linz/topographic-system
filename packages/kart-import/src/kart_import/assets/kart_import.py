import logging
import shutil

from ..command import run_command
from ..config import OUTPUT_DIR, WORKING_THEME_DIR, get_releases, get_themes

logger = logging.getLogger("kart_import")


def kart_import_repo(repo_name: str):
    repo_dir = OUTPUT_DIR / repo_name
    sentinel = repo_dir / ".imported"

    themes = [t for t in get_themes() if t.target_repo == repo_name]
    releases = get_releases()

    # Init the Kart repo if it doesn't exist
    if (repo_dir / ".kart").exists():
        logger.info(f"Removing existing Kart repo at {repo_dir}")
        shutil.rmtree(repo_dir)

    repo_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Initializing Kart repo at {repo_dir}")
    run_command(["kart", "init", "."], cwd=str(repo_dir))

    for release in releases:
        # Set commit date using standard Git environment variables
        env = {
            "GIT_AUTHOR_DATE": release.until.isoformat(),
            "GIT_COMMITTER_DATE": release.until.isoformat(),
        }

        for theme in themes:
            input_file = WORKING_THEME_DIR / f"release_{release.id}" / f"{theme.name}.geojson"
            if not input_file.exists():
                logger.warning(f"Theme file not found: {input_file}. Skipping import.")
                continue

            logger.info(f"Importing {theme.name} for release {release.id} into Kart repo {repo_name}")

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
            run_command(cmd, cwd=str(repo_dir), env=env, allow_error="No changes to commit")

        # Tag the release (use -f to allow overwriting during re-runs) in this repository
        tag_name = f"release_{release.id}"
        logger.info(f"Tagging commit as {tag_name} in {repo_dir}")
        run_command(["kart", "tag", "-f", tag_name], cwd=str(repo_dir))

    logger.info(f"All releases have been successfully imported and tagged in Kart repo {repo_name}.")
    sentinel.touch()
    return str(repo_dir)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m kart_import.assets.kart_import <repo_name>")
        sys.exit(1)
    kart_import_repo(sys.argv[1])
