import logging

from ..command import run_command
from ..config import (
    SOURCE_DIR,
    get_dataset_by_name,
)
from ..env import env_use_bundle
from ..git.bundle import download_and_clone_from_bundle
from ..log import log_context

logger = logging.getLogger("kart_import")


def clone_dataset(dataset_name: str):
    td = get_dataset_by_name(dataset_name)
    dataset_source = td.source
    SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    target_dir = SOURCE_DIR / dataset_name
    sentinel = target_dir / ".cloned"

    if (target_dir / ".kart").exists():
        logger.info("already exists.", extra={"path": target_dir})
        sentinel.touch()
        return

    # Use the bundle if we can to prevent extra pulls
    if env_use_bundle():
        bundle_target = SOURCE_DIR / f"{dataset_name}.bundle"
        try:
            download_and_clone_from_bundle(bundle_target, dataset_name, target_dir)
            return
        except Exception as e:
            logger.warning(
                f"Failed to download and clone from bundle for {dataset_name}: {e}. Falling back to direct kart clone."
            )

    # Clone directly from the source
    run_command(["kart", "clone", f"{dataset_source}", str(target_dir), "--no-checkout"])

    sentinel.touch()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: uv run python -m kart_import.assets.clone <dataset_name>")
        sys.exit(1)
    dataset_name = sys.argv[1]
    with log_context(action="clone", dataset=dataset_name):
        clone_dataset(dataset_name)
