import shutil
import urllib.request
from pathlib import Path

from ..command import run_command
from ..env import env_bundle_url


def download_bundle(dataset_name: str, target_path: Path) -> None:
    """Download a git bundle from CloudFront to a local path."""
    url = env_bundle_url(dataset_name)
    with urllib.request.urlopen(url) as response, open(target_path, "wb") as f:
        shutil.copyfileobj(response, f)


def clone_from_bundle(bundle_path: Path, target_dir: Path) -> None:
    """Clone a git bundle into target_dir."""
    run_command(["git", "clone", str(bundle_path), str(target_dir), "--no-checkout"])


def download_and_clone_from_bundle(bundle_target: Path, dataset_name: str, target_dir: Path) -> None:
    """Download a git bundle from CloudFront to a local path and clone it into target_dir."""
    sentinel = target_dir / ".cloned"
    try:
        download_bundle(dataset_name, bundle_target)
        clone_from_bundle(bundle_target, target_dir)
        sentinel.touch()
        return
    except Exception as e:
        if target_dir.exists():
            shutil.rmtree(target_dir)
        raise e
    finally:
        if bundle_target.exists():
            bundle_target.unlink()
