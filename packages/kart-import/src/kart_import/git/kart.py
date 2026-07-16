import logging
from pathlib import Path

from ..command import run_command

logger = logging.getLogger("kart_import")


def git_to_kart(target_dir: Path):
    """Attempt to convert a git folder to a kart folder,

    This can be removed once the next version of kart allows KART_ALLOW_GIT=1
    """
    git_folder = target_dir / ".git"
    kart_folder = target_dir / ".kart"

    # Already a kart folder
    if kart_folder.exists():
        return

    git_folder.rename(kart_folder)
    git_folder.write_text("gitdir: .kart\n")

    run_command(["git", "config", "-f", ".kart/config", "kart.repostructure.version", "3"], cwd=target_dir)


def is_kart(target_dir: Path):
    """Is the target folder a kart repo, can be swapped to .git with KART_ALLOW_GIT=1"""
    return (target_dir / ".kart").exists()


def get_kart_dataset_id(target_dir: Path):
    kart_dataset_id = run_command(["kart", "data", "ls"], cwd=str(target_dir)).strip()
    if "\n" in kart_dataset_id:
        raise Exception(f"Invalid dataset id: '{target_dir}'")
    return kart_dataset_id
