import logging
from datetime import date

from ...command import run_command
from ...config import SOURCE_DIR, get_source_repo
from ...log import log_context

logger = logging.getLogger("kart_import")


def clone_sentinel_name(day: date | None = None) -> str:
    """Daily clone/fetch sentinel filename (e.g. `.cloned_2026-11-20`), so Snakemake refreshes
    each source repo at most once per day."""
    return f".cloned_{(day or date.today()).isoformat()}"


def clone_repo(repo_name: str, day: date | None = None) -> None:
    entries = get_source_repo(repo_name)
    dataset_url = entries[0].source.url
    SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    target_dir = SOURCE_DIR / repo_name
    sentinel = target_dir / clone_sentinel_name(day)

    if (target_dir / ".git").exists():
        with log_context(action="git_fetch", repo=repo_name):
            run_command(["git", "fetch", "origin"], cwd=str(target_dir))
    else:
        with log_context(action="git_clone", repo=repo_name):
            run_command(["git", "clone", f"{dataset_url}", str(target_dir), "--no-checkout"])

    for old in target_dir.glob(".cloned*"):
        if old != sentinel:
            old.unlink()
    sentinel.touch()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: uv run python -m kart_import.assets.extract.clone <repo_name>")
        sys.exit(1)
    repo = sys.argv[1]
    with log_context(action="clone", repo=repo):
        clone_repo(repo)
