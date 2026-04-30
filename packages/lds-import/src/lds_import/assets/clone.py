from pathlib import Path
import time

from lds_import.config import get_datasets, get_dataset_name, SOURCE_DIR
from dagster import asset, AssetExecutionContext
from lds_import.command import run_command

def should_pull(target_dir: Path): 
    """
        Limit pulls to once per day, so not to overload kart
    """
    # Pull only once a day
    fetch_head = target_dir / ".kart" / "FETCH_HEAD"
    if not fetch_head.exists():
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
        target_dir = SOURCE_DIR / dataset_source
        
        if (target_dir / ".git").exists() or (target_dir / ".kart").exists():
            context.log.info(f"{target_dir} already exists.")
            
            if should_pull(target_dir):
                context.log.info("Attempting 'kart pull'.")
                cmd = ["kart", "pull"]
                run_command(context, cmd, cwd=str(target_dir))
        else:
            # Kart URLS do not contain the layer id
            # kart@data.koordinates.com:linz/nz-airport-polygons-topo-150k
            remote_name = dataset_source.split('-', 1)[1]
            cmd = ["kart", "clone", f"kart@data.koordinates.com:linz/{remote_name}", str(target_dir), "--no-checkout"]
            run_command(context, cmd)
            
        return str(target_dir)
    return _clone_asset

clone_assets = [make_clone_asset(t) for t in get_datasets()]

