import logging
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

from ..command import run_command
from ..config import (
    DATASET_MAP,
    SOURCE_DIR,
    WORKING_EXPORTS_DIR,
)
from ..env import env_bundle_s3_url, env_bundle_url
from ..git.bundle import download_and_clone_from_bundle
from ..git.kart import get_kart_dataset_id, is_kart, source_ref
from ..log import log_context
from ..thread import run_in_thread_pool

logger = logging.getLogger("kart_import")

NETWORK_RETRIES = 3

"""
Take datasets from LINZ Data Service, bundle them into a git .bundle
then store them into AWS S3 for fast access
"""


def should_pull(target_dir: Path):
    """Limit pulls to once per day, so not to overload kart"""
    fetch_head = target_dir / ".kart" / "FETCH_HEAD"
    if not fetch_head.exists():
        fetch_head = target_dir / ".git" / "FETCH_HEAD"

    if not fetch_head.exists():
        return True

    last_pull_time = fetch_head.stat().st_mtime
    return not time.time() - last_pull_time < 24 * 3600


def fetch_bundle_head(dataset_name: str) -> str | None:
    """git bundles start with the ref list in plain text followed by the pack files

    Extract the current HEAD sha if it exists from the first 128KB of the bundle
    """
    remote_bundle_url = env_bundle_url(dataset_name)
    try:
        req = urllib.request.Request(remote_bundle_url, headers={"Range": "bytes=0-131071"})
        with urllib.request.urlopen(req) as response:
            data = response.read(131072)
    except urllib.error.URLError:
        return None

    lines = []
    for line_bytes in data.split(b"\n"):
        line_bytes = line_bytes.rstrip(b"\r")
        if not line_bytes:
            break
        try:
            line = line_bytes.decode("utf-8")
            lines.append(line)
        except UnicodeDecodeError:
            break

    refs = {}
    for line in lines:
        if line.startswith("#") or line.startswith("-") or line.startswith("@"):
            continue
        parts = line.split()
        if len(parts) >= 2:
            sha, ref = parts[0], parts[1]
            refs[ref] = sha

    if "HEAD" in refs:
        return refs["HEAD"]

    raise Exception(f"No HEAD found in bundle refs for {dataset_name}")


# Error markers meaning the local clone and the source no longer share history, e.g. a
# dataset was force-pushed, so a seeded/old clone is unrelated to the current source
# and can never be pulled onto it.
_UNRELATED_HISTORY_MARKERS = ("aren't related", "unrelated histories", "refusing to merge unrelated")


def _fresh_clone(target_dir: Path, dataset_source: str) -> None:
    """Clone `dataset_source` fresh into `target_dir`, removing any existing directory first."""
    if target_dir.exists():
        logger.info("Removing existing source dir before fresh clone", extra={"target": str(target_dir)})
        shutil.rmtree(target_dir)
    run_command(["git", "clone", dataset_source, str(target_dir), "--no-checkout"], retries=NETWORK_RETRIES)


def _pull_or_fresh_clone(target_dir: Path, dataset_source: str) -> None:
    """Delta-pull an existing clone from `dataset_source`; if the histories are unrelated, discard the local clone
    and fresh-clone instead of failing."""
    run_command(["git", "remote", "set-url", "origin", dataset_source], cwd=str(target_dir))
    logger.info("Attempting 'kart pull'.")
    try:
        # `kart pull` handles both .git and .kart layouts while plain `git pull` struggles with kart.
        run_command(["kart", "pull"], cwd=str(target_dir), retries=NETWORK_RETRIES)
    except subprocess.CalledProcessError as e:
        err = f"{e.stderr or ''}{e.output or ''}"
        if any(marker in err for marker in _UNRELATED_HISTORY_MARKERS):
            logger.warning(f"unrelated history for {target_dir.name}. Discarding local clone and fresh-cloning.")
            _fresh_clone(target_dir, dataset_source)
            return
        raise

    # If HEAD no longer matches the source tip, discard and fresh-clone so the bundle is clean.
    head = run_command(["git", "rev-parse", "HEAD"], cwd=str(target_dir), check_error=False).strip()
    tip = run_command(["git", "rev-parse", source_ref(target_dir)], cwd=str(target_dir), check_error=False).strip()
    if head and tip and head != tip:
        logger.warning(
            f"{target_dir.name}: local branch ({head[:8]}) diverged from source tip ({tip[:8]}); fresh-cloning."
        )
        _fresh_clone(target_dir, dataset_source)


def bundle_dataset(dataset_name: str):
    td = DATASET_MAP.get(dataset_name)
    if not td:
        raise ValueError(f"Dataset not found for name: {dataset_name}")
    dataset_source = td.source.url

    SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    target_dir = SOURCE_DIR / dataset_name
    bundle_target = SOURCE_DIR / f"{dataset_name}.bundle"
    sentinel = target_dir / ".bundle_created"

    bundle_head_sha = fetch_bundle_head(dataset_name)

    # Skip bundle if the published bundle already matches the source HEAD.
    if bundle_head_sha:
        ls_remote_out = run_command(["git", "ls-remote", dataset_source, "HEAD"], retries=NETWORK_RETRIES).strip()
        source_head_sha = ls_remote_out.split()[0] if ls_remote_out else None
        logger.info("head", extra={"remote": source_head_sha, "bundle": bundle_head_sha})
        if source_head_sha == bundle_head_sha:
            logger.info(f"CloudFront and Source HEAD match ({source_head_sha}). Skipping clone and bundle.")
            sentinel.touch()
            return

    if is_kart(target_dir):
        if should_pull(target_dir):
            _pull_or_fresh_clone(target_dir, dataset_source)
    else:
        # No local clone present. Seed from the published bundle, then pull. Fall back to a full fresh clone.
        seeded = False
        if bundle_head_sha:
            logger.info(f"Bundle is stale; seeding {dataset_name} from it before pulling the delta.")
            try:
                download_and_clone_from_bundle(bundle_target, dataset_name, target_dir)
                seeded = True
            except (urllib.error.URLError, subprocess.CalledProcessError, OSError) as e:
                logger.warning(f"Seed-from-bundle failed for {dataset_name}; fresh-cloning instead ({e}).")
        if seeded:
            _pull_or_fresh_clone(target_dir, dataset_source)
        else:
            _fresh_clone(target_dir, dataset_source)

    run_command(["git", "-C", str(target_dir), "bundle", "create", str(bundle_target), "--all"])

    head_sha = run_command(["git", "rev-parse", "HEAD"], cwd=str(target_dir)).strip()

    # Upload to S3 via aws s3 cp
    s3_url = env_bundle_s3_url()

    logger.info(f"Uploading bundle to {s3_url}...")
    run_command(["aws", "s3", "cp", str(bundle_target), f"{s3_url}{dataset_name}.bundle"], retries=NETWORK_RETRIES)

    kart_dataset_id = td.source.dataset or get_kart_dataset_id(target_dir)
    per_commit_dir = WORKING_EXPORTS_DIR / dataset_name
    per_commit_dir.mkdir(parents=True, exist_ok=True)

    def export_commit(sha: str) -> bool:
        s3_key = f"{s3_url}{dataset_name}/{sha}.json.gz"
        result = run_command(["aws", "s3", "ls", s3_key], check_error=False)
        if result.strip():
            return False
        json_export = str(per_commit_dir / f"{sha}.json")
        out = run_command(
            ["kart", "export", "--overwrite", "--ref", sha, kart_dataset_id, json_export],
            cwd=str(target_dir),
            allow_error="No such dataset",
        )
        if "No such dataset" in out:
            return False
        run_command(["gzip", "-f", "-9", json_export])
        run_command(["aws", "s3", "cp", f"{json_export}.gz", s3_key], retries=NETWORK_RETRIES)
        return True

    all_commits = run_command(
        ["git", "log", "--all", "--pretty=format:%H", "--reverse"], cwd=str(target_dir)
    ).splitlines()
    results = run_in_thread_pool(
        func=export_commit,
        items=all_commits,
        thread_count=4,
    )
    exported = sum(results)
    skipped = len(results) - exported

    logger.info(
        f"Successfully uploaded {dataset_name}.bundle (head: {head_sha}, exported: {exported}, skipped: {skipped})"
    )
    sentinel.touch()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m kart_import.assets.bundle <dataset_name>")
        sys.exit(1)
    dataset = sys.argv[1]
    with log_context(action="bundle", dataset=dataset):
        try:
            bundle_dataset(dataset)
        except Exception:
            # Emit the traceback through the JSON logger so it lands in the structured
            # log stream. Snakemake captures a failing job's raw stderr but doesn't
            # inline it in its main log, so an uncaught traceback would be lost.
            logger.exception(f"bundle failed for {dataset}")
            sys.exit(1)
