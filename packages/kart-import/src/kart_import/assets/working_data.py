import json
import logging
import os

from ..command import run_command
from ..config import EXPORT_DIR, SOURCE_DIR, SOURCE_REPO_MAP, WORKING_DIR
from ..env import env_skip_upload, env_working_data_s3_url
from ..export_paths import release_config_hash, source_commit_hash
from ..log import log_context
from .extract.clone import clone_sentinel_name

logger = logging.getLogger("kart_import")

NETWORK_RETRIES = 3

"""
Hand off EXPORT_DIR (`data/export`) between the two pipeline stages:
`extract_all` (clone + export parquets, needs an LDS-registered SSH key) produces it;
`upload` / `download` sync it via S3 so `process_all` (lifecycle/prepare/transform/theme/import/push)
can run with only AWS credentials and no source clones.
"""


def _build_summary() -> dict:
    """A compact manifest of every dataset's/lookup's release map currently in EXPORT_DIR (see
    `export_paths.write_releases_map`), plus the same `config`/`commit` hashes they each already
    carry, so a release definition change or a new pipeline version is caught even before either
    has changed any dataset's resolved commits. `download_working_data` compares this (fetched
    from S3) against its last-synced copy to decide whether the much bigger `aws s3 sync` of the
    actual exports is even worth doing."""
    entries = {}
    for releases_file in EXPORT_DIR.glob("*/*.releases.json"):
        name = releases_file.name.removesuffix(".releases.json")
        entries[name] = json.loads(releases_file.read_text())
    return {
        "config": release_config_hash(),
        "commit": source_commit_hash(),
        "entries": entries,
    }


def upload_working_data() -> None:
    """Write `summary.json` locally, then (unless `KART_IMPORT_SKIP_UPLOAD`) sync EXPORT_DIR and
    that summary up to S3 (see `_build_summary`)."""
    # `aws s3 sync` requires its local source to exist; nothing creates EXPORT_DIR
    # at all if the requested scope (KART_IMPORT_THEME/_RELEASE) happens to resolve to zero datasets.
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    WORKING_DIR.mkdir(parents=True, exist_ok=True)
    summary_file = WORKING_DIR / "summary.json"
    summary_file.write_text(json.dumps(_build_summary()))

    if env_skip_upload():
        logger.info("KART_IMPORT_SKIP_UPLOAD set; leaving working data local, not pushing to S3")
        return

    s3_url = env_working_data_s3_url()
    run_command(["aws", "s3", "sync", str(EXPORT_DIR), f"{s3_url}export"], retries=NETWORK_RETRIES)
    run_command(["aws", "s3", "cp", str(summary_file), f"{s3_url}summary.json"], retries=NETWORK_RETRIES)

    logger.info("uploaded working data", extra={"s3_url": s3_url})


def download_working_data() -> None:
    """Always fetch `summary.json` from S3 and compare it to the last-synced copy; only run the
    (much more expensive) `aws s3 sync` of the actual exports when it's changed."""
    s3_url = env_working_data_s3_url()
    WORKING_DIR.mkdir(parents=True, exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    remote_summary_file = WORKING_DIR / ".remote_summary.json"
    run_command(["aws", "s3", "cp", f"{s3_url}summary.json", str(remote_summary_file)], retries=NETWORK_RETRIES)
    remote_summary = remote_summary_file.read_text()
    remote_summary_file.unlink()

    local_summary_file = WORKING_DIR / "summary.json"
    if local_summary_file.exists() and local_summary_file.read_text() == remote_summary:
        logger.info("working data already up to date; skipping sync", extra={"s3_url": s3_url})
    else:
        run_command(["aws", "s3", "sync", f"{s3_url}export", str(EXPORT_DIR)], retries=NETWORK_RETRIES)
        local_summary_file.write_text(remote_summary)
        logger.info("downloaded working data", extra={"s3_url": s3_url})

    # No real clone is made here -- the downloaded exports/schema sidecars/release maps are
    # everything Stage 2 needs. An ancient mtime keeps Snakemake from deciding `clone`/`export`
    # must re-run just because `.cloned` didn't exist when this ran: their own outputs
    # (`.sentinel/<entry>.exported`, already downloaded) are newer than it, so they're left alone.
    sentinel_name = clone_sentinel_name()
    for repo_name in SOURCE_REPO_MAP:
        repo_dir = SOURCE_DIR / repo_name
        repo_dir.mkdir(parents=True, exist_ok=True)
        sentinel = repo_dir / sentinel_name
        sentinel.touch()
        os.utime(sentinel, (0, 0))


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

