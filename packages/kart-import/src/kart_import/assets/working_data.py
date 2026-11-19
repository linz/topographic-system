import logging
import os

from ..command import run_command
from ..config import DATASET_MAP, SOURCE_DIR, SOURCE_REPO_MAP, WORKING_DIR, WORKING_EXPORTS_DIR, WORKING_LIFECYCLE_DIR
from ..env import env_working_data_s3_url
from ..log import log_context
from .export import relink_dataset_releases

logger = logging.getLogger("kart_import")

NETWORK_RETRIES = 3

"""
Hand off `data/working/export` and `data/working/lifecycle` between the two pipeline stages:
`export_all` (clone+export+lifecycle, needs an LDS-registered SSH key) uploads them here;
`process_all` (prepare/transform/theme/import/push, needs only AWS credentials) downloads them.
"""


def upload_working_data() -> None:
    s3_url = env_working_data_s3_url()
    run_command(["aws", "s3", "sync", str(WORKING_EXPORTS_DIR), f"{s3_url}export"], retries=NETWORK_RETRIES)
    run_command(["aws", "s3", "sync", str(WORKING_LIFECYCLE_DIR), f"{s3_url}lifecycle"], retries=NETWORK_RETRIES)
    WORKING_DIR.mkdir(parents=True, exist_ok=True)
    (WORKING_DIR / ".exported").touch()
    logger.info("uploaded working data", extra={"s3_url": s3_url})


def download_working_data() -> None:
    s3_url = env_working_data_s3_url()
    WORKING_EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    WORKING_LIFECYCLE_DIR.mkdir(parents=True, exist_ok=True)
    run_command(["aws", "s3", "sync", f"{s3_url}export", str(WORKING_EXPORTS_DIR)], retries=NETWORK_RETRIES)
    run_command(["aws", "s3", "sync", f"{s3_url}lifecycle", str(WORKING_LIFECYCLE_DIR)], retries=NETWORK_RETRIES)

    for dataset_name in DATASET_MAP:
        relink_dataset_releases(dataset_name)

    # No real clone is made here -- the downloaded exports/lifecycle/schema sidecars are
    # everything the rest of the pipeline needs. An ancient mtime keeps Snakemake from deciding
    # `clone`/`export`/`export_lookup` must re-run just because `.cloned` didn't exist when this
    # ran: their own outputs (already downloaded) are newer than it, so they're left alone.
    for repo_name in SOURCE_REPO_MAP:
        repo_dir = SOURCE_DIR / repo_name
        repo_dir.mkdir(parents=True, exist_ok=True)
        sentinel = repo_dir / ".cloned"
        sentinel.touch()
        os.utime(sentinel, (0, 0))

    logger.info("downloaded working data", extra={"s3_url": s3_url})


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2 or sys.argv[1] not in ("upload", "download"):
        print("Usage: python -m kart_import.assets.working_data upload|download")
        sys.exit(1)

    with log_context(action=f"working_data_{sys.argv[1]}"):
        if sys.argv[1] == "upload":
            upload_working_data()
        else:
            download_working_data()
