import logging
import shutil
from pathlib import Path

from ..command import run_command
from ..config import (
    SOURCE_DIR,
    get_source_repo,
)
from ..env import env_use_bundle
from ..git.bundle import download_and_clone_from_bundle
from ..git.kart import is_kart, ref_has_dataset, source_ref
from ..log import log_context

logger = logging.getLogger("kart_import")


def _clone_has_dataset(target_dir: Path, wanted: list[str]) -> bool:
    """Whether the clone actually contains every configured dataset at its source tip.

    Only checked when the dataset ids are known (multi-dataset repos declare ``source.dataset``);
    single-dataset koordinates repos (``wanted`` empty) can be used as-is.
    Guards against a clone that is the wrong repo or a stale bundle that doesn't contain the dataset.
    """
    if not wanted:
        return True
    ref = source_ref(target_dir)
    return all(ref_has_dataset(target_dir, ref, dataset_id) for dataset_id in wanted)


def clone_repo(repo_name: str) -> None:
    entries = get_source_repo(repo_name)
    dataset_url = entries[0].source.url
    wanted = sorted({e.source.dataset for e in entries if e.source.dataset})
    SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    target_dir = SOURCE_DIR / repo_name
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

    # Use the bundle if we can to prevent extra pulls, then catch up with the real source
    # repo since the published bundle may be stale.
    if env_use_bundle():
        bundle_target = SOURCE_DIR / f"{repo_name}.bundle"
        try:
            download_and_clone_from_bundle(bundle_target, repo_name, target_dir)
            logger.info(f"{repo_name}: seeded from bundle; pulling latest from source.")
            with log_context(action="git_remote_pull", repo=repo_name):
                run_command(["git", "remote", "set-url", "origin", dataset_url], cwd=str(target_dir))
                run_command(["kart", "pull"], cwd=str(target_dir))
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
                f"Failed to download and clone from bundle for {repo_name}: {e}. Falling back to direct git clone."
            )
            if target_dir.exists():
                shutil.rmtree(target_dir)

    # switched to `git clone`. `kart` supports git repos since v0.17.1
    with log_context(action="git_clone", repo=repo_name):
        run_command(["git", "clone", f"{dataset_url}", str(target_dir), "--no-checkout"])

    sentinel.touch()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: uv run python -m kart_import.assets.clone <repo_name>")
        sys.exit(1)
    repo = sys.argv[1]
    with log_context(action="clone", repo=repo):
        clone_repo(repo)
