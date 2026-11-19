import logging

from ..command import run_command
from ..config import (
    SOURCE_DIR,
    TRANSFORM_SUFFIX,
    WORKING_TRANSFORM_DIR,
    ThemeDataset,
    get_source_repo,
)
from ..git.kart import is_kart
from ..log import log_context
from .export import release_commit_map

logger = logging.getLogger("kart_import")

"""
Keep an already-cloned local source repo current by pulling directly from the real source.
"""


def _dataset_names(repo_name: str) -> list[str]:
    """Every dataset sourced from `repo_name` (not a lookup - see `invalidate_stale_transforms`)."""
    return [entry.name for entry in get_source_repo(repo_name) if isinstance(entry, ThemeDataset)]


def invalidate_stale_transforms(repo_name: str, before: dict[str, dict[int, str | None]]) -> None:
    """Delete a dataset's `transform` output wherever this pull moved its resolved source commit,
    so the next pipeline run recomputes exactly what changed.

    `transform_dataset_release` skips a release whose output file already exists - correct for an
    unchanged commit, but it has no way to notice that a newly pulled commit makes an existing
    file stale, since that file is keyed by release id, not by the commit it was built from.
    `export`'s own per-commit cache doesn't have this problem (a new commit is a new filename,
    so it's a natural cache miss); `transform`'s is what needs this explicit invalidation.

    Nothing downstream of `transform` needs the same treatment: `theme_release`'s merge and
    `kart_theme`'s import both redo their work unconditionally whenever Snakemake re-runs them
    (which it does, once this makes the transform file's mtime newer than theirs), rather than
    skipping on existence the way `transform` does.

    Lookups aren't covered - they're exported and prepared per commit already, so a newly pulled
    commit is a cache miss there on its own, the same way `export`'s dataset cache is.
    """
    for dataset_name in _dataset_names(repo_name):
        old_commits = before.get(dataset_name, {})
        new_commits = release_commit_map(dataset_name)
        for release_id, commit in new_commits.items():
            if old_commits.get(release_id) == commit:
                continue
            target = WORKING_TRANSFORM_DIR / f"release_{release_id}" / f"{dataset_name}{TRANSFORM_SUFFIX}"
            if target.exists() or target.is_symlink():
                target.unlink()
                logger.info(
                    "invalidated stale transform",
                    extra={
                        "dataset": dataset_name,
                        "release": release_id,
                        "old_commit": old_commits.get(release_id),
                        "new_commit": commit,
                    },
                )


def pull_repo(repo_name: str) -> None:
    dataset_source = get_source_repo(repo_name)[0].source.url

    target_dir = SOURCE_DIR / repo_name
    sentinel = target_dir / ".pulled"

    if not is_kart(target_dir):
        logger.warning("no local clone found; run clone_all first", extra={"path": str(target_dir)})
        target_dir.mkdir(parents=True, exist_ok=True)
        sentinel.touch()
        return

    before = {name: release_commit_map(name) for name in _dataset_names(repo_name)}

    logger.info(f"{repo_name}: pulling latest from source.", extra={"path": str(target_dir)})
    with log_context(action="git_remote_pull", repo=repo_name):
        run_command(["git", "remote", "set-url", "origin", dataset_source], cwd=str(target_dir))
        run_command(["kart", "pull"], cwd=str(target_dir))

    invalidate_stale_transforms(repo_name, before)

    sentinel.touch()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: uv run python -m kart_import.assets.pull <repo_name>")
        sys.exit(1)
    repo = sys.argv[1]
    with log_context(action="pull", repo=repo):
        pull_repo(repo)
