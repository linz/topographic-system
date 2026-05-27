import logging
import shutil
from pathlib import Path

from kart_import.log import log_context

from ..command import run_command
from ..config import OUTPUT_DIR, get_themes

logger = logging.getLogger("kart_import")


def kart_import_repo(repo_name: str):

    repo_dir = OUTPUT_DIR / repo_name
    imported_marker = repo_dir / ".imported"

    themes = [t for t in get_themes() if t.target_repo == repo_name]

    if not themes:
        logger.warning(f"No themes found for repo {repo_name}")
        return

    # Ensure a clean state before initializing the repo
    if repo_dir.exists():
        logger.info(f"Removing existing repo directory", extra={"target": str(repo_dir)})
        shutil.rmtree(repo_dir)

    repo_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Initializing Git repo", extra={"target": str(repo_dir)})
    run_command(["git", "init", "."], cwd=str(repo_dir))
    run_command(["git", "config", "commit.gpgsign", "false"], cwd=str(repo_dir))
    
    # Enable cone-mode sparse checkout to speed up pulls and simulate --no-checkout
    run_command(["git", "sparse-checkout", "init", "--cone"], cwd=str(repo_dir))
    run_command(["git", "sparse-checkout", "set"], cwd=str(repo_dir))

    # Fetch all bundles into separate branches
    for theme in themes:
        bundle_file = OUTPUT_DIR / f"{theme.name}.bundle"
        if not bundle_file.exists():
            raise Exception(f"Bundle file not found: {bundle_file}")

        logger.info(f"Fetching bundle", extra={"bundle": str(bundle_file), "theme": theme.name})
        run_command(["git", "fetch", str(bundle_file), f"master:{theme.name}"], cwd=str(repo_dir))

    # Get all commits from all fetched branches sorted by author timestamp
    import subprocess
    cmd_log = ["git", "log", "--all", "--format=%at %H"]
    result = subprocess.run(cmd_log, cwd=str(repo_dir), capture_output=True, text=True, check=True)
    
    # Sort lines by timestamp (the first column) and extract the commit hash
    commits = []
    for line in sorted(result.stdout.strip().split("\n")):
        if line:
            commits.append(line.split(" ")[1])

    logger.info(f"Cherry-picking {len(commits)} commits to create a chronologically ordered linear history")

    # Cherry-pick all commits into the current master branch sequentially
    if commits:
        cmd_cp = ["git", "cherry-pick", "--allow-empty", "--strategy-option=theirs"] + commits
        run_command(cmd_cp, cwd=str(repo_dir))

    # Clean up the temporary fetched branches
    for theme in themes:
        run_command(["git", "branch", "-D", theme.name], cwd=str(repo_dir))

    # Touch the .imported marker for snakemake
    imported_marker.touch()

    logger.info(f"All bundles merged for repo {repo_name}")
    return str(repo_dir)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m kart_import.assets.kart_import_repo <repo_name>")
        sys.exit(1)
    with log_context(action="kart_import_repo", repo=sys.argv[1]):
        kart_import_repo(sys.argv[1])
