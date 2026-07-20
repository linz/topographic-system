import logging
import shutil
from pathlib import Path

from ..command import run_command
from ..config import (
    SOURCE_DIR,
    get_source_entry,
)
from ..env import env_use_bundle
from ..git.bundle import download_and_clone_from_bundle
from ..git.kart import is_kart, ref_has_dataset, source_ref
from ..log import log_context

logger = logging.getLogger("kart_import")


def _clone_has_dataset(target_dir: Path, wanted: str | None) -> bool:
    """Whether the clone actually contains the configured dataset at its source tip.

    Only checked when the dataset id is known (multi-dataset repos declare ``source.dataset``);
    single-dataset koordinates repos (``wanted is None``) can be used as-is.
    Guards against a clone that is the wrong repo or a stale bundle that doesn't contain the dataset.
    """
    if not wanted:
        return True
    return ref_has_dataset(target_dir, source_ref(target_dir), wanted)


def clone_dataset(dataset_name: str) -> None:
    td = get_source_entry(dataset_name)
    dataset_url = td.source.url
    wanted = td.source.dataset
    SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    target_dir = SOURCE_DIR / dataset_name
    sentinel = target_dir / ".cloned"

    if is_kart(target_dir):
        if _clone_has_dataset(target_dir, wanted):
            logger.info("already exists.", extra={"path": target_dir})
            sentinel.touch()
            return
        logger.warning(
            "existing clone does not contain the configured dataset; re-cloning from source",
            extra={"path": str(target_dir), "dataset": wanted},
        )
        shutil.rmtree(target_dir)

    # Use the bundle if we can to prevent extra pulls
    if env_use_bundle():
        bundle_target = SOURCE_DIR / f"{dataset_name}.bundle"
        try:
            download_and_clone_from_bundle(bundle_target, dataset_name, target_dir)
            if _clone_has_dataset(target_dir, wanted):
                return
            logger.warning(
                "bundle clone does not contain the configured dataset (stale bundle?); "
                "falling back to direct git clone",
                extra={"dataset": wanted},
            )
            shutil.rmtree(target_dir)
        except Exception as e:
            logger.warning(
                f"Failed to download and clone from bundle for {dataset_name}: {e}. Falling back to direct git clone."
            )

    # switched to `git clone`. `kart` supports git repos since v0.17.1
    run_command(["git", "clone", f"{dataset_url}", str(target_dir), "--no-checkout"])

    sentinel.touch()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: uv run python -m kart_import.assets.clone <dataset_name>")
        sys.exit(1)
    dataset = sys.argv[1]
    with log_context(action="clone", dataset=dataset):
        clone_dataset(dataset)
