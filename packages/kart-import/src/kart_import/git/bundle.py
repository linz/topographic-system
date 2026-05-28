import shutil
import urllib.request
from pathlib import Path

from ..command import run_command
from ..env import env_bundle_url
from .kart import git_to_kart


def download_bundle(dataset_name: str, target_path: Path) -> None:
    """Download a git bundle from CloudFront to a local path.

    Raises urllib.error.URLError on network failure or a non-2xx response.
    """
    url = env_bundle_url(dataset_name)
    with urllib.request.urlopen(url) as response, open(target_path, "wb") as f:
        shutil.copyfileobj(response, f)


def clone_from_bundle(bundle_path: Path, target_dir: Path) -> None:
    """Clone a git bundle into target_dir and convert it to a kart repository."""
    run_command(["git", "clone", str(bundle_path), str(target_dir), "--no-checkout"])
    git_to_kart(target_dir)
