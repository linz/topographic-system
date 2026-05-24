import time
from pathlib import Path

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from ..command import run_command
from ..config import (
    SOURCE_DIR,
    get_bundle_url,
    get_dataset_name,
    get_datasets,
    is_use_bundle,
)
from ..git.kart import git_to_kart


def should_pull(target_dir: Path):
    """
    Limit pulls to once per day, so not to overload kart
    """
    fetch_head = target_dir / ".git" / "FETCH_HEAD"

    if not fetch_head.exists():
        return True

    last_pull_time = fetch_head.stat().st_mtime
    return not time.time() - last_pull_time < 24 * 3600


def make_clone_asset(dataset_source: str):
    dataset_name = get_dataset_name(dataset_source)

    @asset(name=f"clone_{dataset_name}", group_name="kart")
    def _clone_asset(context: AssetExecutionContext):
        SOURCE_DIR.mkdir(parents=True, exist_ok=True)
        target_dir = SOURCE_DIR / dataset_name

        if (target_dir / ".git").exists() or (target_dir / ".kart").exists():
            context.log.info(f"{target_dir} already exists.")
            return MaterializeResult(metadata={"location": MetadataValue.path(str(target_dir))})

        # Use the bundle if we can to prevent extra pulls
        if is_use_bundle():
            bundle_target = SOURCE_DIR / f"{dataset_name}.bundle"
            cmd = ["curl", "-L", get_bundle_url(dataset_name), "-o", str(bundle_target)]
            run_command(context, cmd)

            cmd = ["kart", "git", "clone", str(bundle_target), str(target_dir), "--no-checkout"]
            run_command(context, cmd)

            git_to_kart(context, target_dir)
            bundle_target.unlink()
            return MaterializeResult(metadata={"location": MetadataValue.path(str(target_dir))})

        # Clone directly from the source
        cmd = ["kart", "clone", f"{dataset_source}", str(target_dir), "--no-checkout"]
        run_command(context, cmd)

        return MaterializeResult(metadata={"location": MetadataValue.path(str(target_dir))})

    return _clone_asset


clone_assets = [make_clone_asset(t) for t in get_datasets()]


@asset(deps=clone_assets)
def clone_all(context: AssetExecutionContext):
    """Wait for all clone assets to be created and checked."""
    context.log.info("All clones successfully processed.")
    return MaterializeResult(metadata={"status": MetadataValue.text("ok")})
