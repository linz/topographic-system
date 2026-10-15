import logging

from ..command import run_command
from ..config import DATASET_MAP, SOURCE_DIR
from ..git.kart import is_kart
from ..log import log_context
from .bundle import fetch_bundle_head

logger = logging.getLogger("kart_import")

"""
Keep an already-cloned local source repo current, cheaply: compare the published bundle's
HEAD sha (one small ranged HTTP read, see `fetch_bundle_head`) against the local clone's
HEAD, and only hit the real source with a `kart pull` when they've diverged.
"""


def pull_dataset(dataset_name: str) -> None:
    td = DATASET_MAP.get(dataset_name)
    if not td:
        raise ValueError(f"Dataset not found for name: {dataset_name}")
    dataset_source = td.source.url

    target_dir = SOURCE_DIR / dataset_name
    sentinel = target_dir / ".pulled"

    if not is_kart(target_dir):
        logger.warning("no local clone found; run clone_all first", extra={"path": str(target_dir)})
        target_dir.mkdir(parents=True, exist_ok=True)
        sentinel.touch()
        return

    bundle_head_sha = fetch_bundle_head(dataset_name)
    local_head_sha = run_command(["git", "rev-parse", "HEAD"], cwd=str(target_dir), check_error=False).strip()

    if bundle_head_sha and local_head_sha and bundle_head_sha == local_head_sha:
        logger.info(f"{dataset_name} already matches the published bundle ({local_head_sha}).")
        sentinel.touch()
        return

    logger.info(
        f"{dataset_name}: local clone ({local_head_sha[:8] if local_head_sha else 'unknown'}) is behind "
        f"the published bundle ({bundle_head_sha[:8] if bundle_head_sha else 'unknown'}); pulling latest.",
        extra={"path": str(target_dir)},
    )
    run_command(["git", "remote", "set-url", "origin", dataset_source], cwd=str(target_dir))
    run_command(["kart", "pull"], cwd=str(target_dir))

    sentinel.touch()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: uv run python -m kart_import.assets.pull <dataset_name>")
        sys.exit(1)
    dataset = sys.argv[1]
    with log_context(action="pull", dataset=dataset):
        pull_dataset(dataset)
