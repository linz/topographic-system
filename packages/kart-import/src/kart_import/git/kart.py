from pathlib import Path
from dagster import AssetExecutionContext

from kart_import.command import run_command


def git_to_kart(context: AssetExecutionContext, target_dir: Path):
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

    run_command(context, ["git", "config", "-f", ".kart/config", "kart.repostructure.version", "3"], cwd=target_dir)


def is_kart(target_dir: Path):
    """Is the target folder a kart repo, can be swapped to .git with KART_ALLOW_GIT=1"""
    return (target_dir / ".kart").exists()
