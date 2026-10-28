import logging

from ..command import run_command
from ..config import SOURCE_DIR, get_source_repo
from ..git.kart import is_kart
from ..log import log_context

logger = logging.getLogger("kart_import")

"""
Keep an already-cloned local source repo current by pulling directly from the real source.
Bundles are only a fast path for the initial clone; `pull_all` always goes straight to the
source repo so it never trusts a bundle that may be stale.
"""


def pull_repo(repo_name: str) -> None:
    dataset_source = get_source_repo(repo_name)[0].source.url

    target_dir = SOURCE_DIR / repo_name
    sentinel = target_dir / ".pulled"

    if not is_kart(target_dir):
        logger.warning("no local clone found; run clone_all first", extra={"path": str(target_dir)})
        target_dir.mkdir(parents=True, exist_ok=True)
        sentinel.touch()
        return

    logger.info(f"{repo_name}: pulling latest from source.", extra={"path": str(target_dir)})
    with log_context(action="git_remote_pull", repo=repo_name):
        run_command(["git", "remote", "set-url", "origin", dataset_source], cwd=str(target_dir))
        run_command(["kart", "pull"], cwd=str(target_dir))

    sentinel.touch()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: uv run python -m kart_import.assets.pull <repo_name>")
        sys.exit(1)
    repo = sys.argv[1]
    with log_context(action="pull", repo=repo):
        pull_repo(repo)
