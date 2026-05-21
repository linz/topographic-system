from pathlib import Path
import time

from ..config import (
    get_bundle_url,
    get_datasets,
    get_dataset_name,
    SOURCE_DIR,
    is_use_bundle,
)
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
from ..command import run_command


def should_pull(target_dir: Path):
    """
    Limit pulls to once per day, so not to overload kart
    """
    fetch_head = target_dir / ".git" / "FETCH_HEAD"

    if not fetch_head.exists():
        return True

    last_pull_time = fetch_head.stat().st_mtime
    if time.time() - last_pull_time < 24 * 3600:
        return False

    return True


def make_clone_asset(dataset_source: str):
    dataset_name = get_dataset_name(dataset_source)

    @asset(name=f"clone_{dataset_name}", group_name="kart")
    def _clone_asset(context: AssetExecutionContext):
        SOURCE_DIR.mkdir(parents=True, exist_ok=True)
        target_dir = SOURCE_DIR / dataset_name

        if (target_dir / ".git").exists() or (target_dir / ".kart").exists():
            context.log.info(f"{target_dir} already exists.")

            if should_pull(target_dir):
                context.log.info("Attempting 'kart pull'.")
                cmd = ["kart", "pull"]
                run_command(context, cmd, cwd=str(target_dir))
        else:
            cmd = [
                "git",
                "clone",
                f"{dataset_source}",
                str(target_dir),
                "--no-checkout",
            ]

            if is_use_bundle():
                cmd.append(f"--bundle-uri={get_bundle_url(dataset_name)}")
            run_command(context, cmd)

        return MaterializeResult(
            metadata={
                "location": MetadataValue.path(str(target_dir)),
            }
        )

    return _clone_asset


clone_assets = [make_clone_asset(t) for t in get_datasets()]


@asset(deps=clone_assets)
def clone_all(context: AssetExecutionContext):
    """Wait for all clone assets to be created and checked."""
    context.log.info("All clones successfully processed.")
    return MaterializeResult(metadata={"status": MetadataValue.text("ok")})
