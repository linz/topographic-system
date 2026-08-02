import logging
from pathlib import Path

from ..command import run_command

logger = logging.getLogger("kart_import")


def is_kart(target_dir: Path):
    """Is the target folder a cloned kart/git repo."""
    return (target_dir / ".kart").exists() or (target_dir / ".git").exists()


def get_kart_dataset_id(target_dir: Path):
    kart_dataset_id = run_command(["kart", "data", "ls"], cwd=str(target_dir)).strip()
    if "\n" in kart_dataset_id:
        raise Exception(f"Invalid dataset id: '{target_dir}'")
    return kart_dataset_id


def source_ref(repo_dir: Path) -> str:
    """The ref representing the source's *current* tip.

    Prefers the fetched remote-tracking ref, so a stale or split-brain local branch (e.g. a
    bundle-seeded clone whose local ``master`` lags ``origin/master``) doesn't make callers
    resolve outdated commits. Falls back to ``HEAD`` for repos with no remote-tracking refs.
    """
    for ref in ("origin/HEAD", "origin/master", "origin/main"):
        out = run_command(["git", "rev-parse", "--verify", "--quiet", ref], cwd=str(repo_dir), check_error=False)
        if out.strip():
            return ref
    return "HEAD"


def ref_has_dataset(repo_dir: Path, ref: str, dataset_id: str) -> bool:
    """Whether ``dataset_id`` is a top-level dataset tree at ``ref`` (used to detect a clone
    that is the wrong repo / a stale bundle that doesn't actually contain the dataset)."""
    out = run_command(["git", "ls-tree", "--name-only", ref], cwd=str(repo_dir), check_error=False)
    return dataset_id in out.split()
