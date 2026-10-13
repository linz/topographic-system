import logging
import shutil

from kart_import.log import log_context

from ..command import run_command
from ..config import OUTPUT_DIR, get_themes
from ..git.linearise import linearise

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
        logger.info("Removing existing repo directory", extra={"target": str(repo_dir)})
        shutil.rmtree(repo_dir)

    repo_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Initializing Git repo", extra={"target": str(repo_dir)})
    # -b master so HEAD tracks the branch linearise builds, whatever init.defaultBranch is
    run_command(["git", "init", "-b", "master", "."], cwd=str(repo_dir))
    run_command(["git", "config", "commit.gpgsign", "false"], cwd=str(repo_dir))

    # Enable cone-mode sparse checkout to speed up pulls and simulate --no-checkout
    run_command(["git", "sparse-checkout", "init", "--cone"], cwd=str(repo_dir))
    run_command(["git", "sparse-checkout", "set"], cwd=str(repo_dir))

    # Fetch all bundles into separate branches
    for theme in themes:
        bundle_file = OUTPUT_DIR / f"{theme.name}.bundle"
        if not bundle_file.exists():
            raise Exception(f"Bundle file not found: {bundle_file}")

        logger.info("Fetching bundle", extra={"bundle": str(bundle_file), "theme": theme.name})
        run_command(["git", "fetch", str(bundle_file), f"master:{theme.name}"], cwd=str(repo_dir))

    # Keep the branch each commit came from, so the replay knows which subtree it owns
    dated = []
    for theme in themes:
        result = run_command(["git", "log", theme.name, "--format=%at|%H"], cwd=str(repo_dir))
        for line in result.strip().split("\n"):
            if line:
                dated.append((line, theme.name))

    # Sort lines by timestamp (the first column) and extract the commit hash
    commits = [(theme_name, line.split("|")[1]) for line, theme_name in sorted(dated)]

    logger.info(f"Replaying {len(commits)} commits to create a chronologically ordered linear history")

    if not commits:
        raise Exception("No commits found")

    # The themes are disjoint, so this needs no merging -- see kart_import.git.linearise
    linearise(repo_dir, commits)

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
