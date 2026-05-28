import logging
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
from ..git.bundle import clone_from_bundle, download_bundle
from ..git.kart import get_kart_dataset_id
from ..log import log_context
from ..thread import run_in_thread_pool

logger = logging.getLogger("kart_import")

"""
Take datasets from the linz data service, bundle them into a git .bundle
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


def bundle_dataset(dataset_name: str):
    td = DATASET_MAP.get(dataset_name)
    if not td:
        raise ValueError(f"Dataset not found for name: {dataset_name}")
    dataset_source = td.source

    SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    target_dir = SOURCE_DIR / dataset_name
    bundle_path = SOURCE_DIR / f"{dataset_name}.bundle"
    sentinel = target_dir / ".bundle_created"

    bundle_head = fetch_bundle_head(dataset_name)

    # Skip bundle creation if the current bundle and remote are the same hash
    if bundle_head:
        ls_remote_out = run_command(["git", "ls-remote", dataset_source, "HEAD"]).strip()
        source_head_sha = ls_remote_out.split()[0] if ls_remote_out else None
        logger.info("head", extra={"remote": source_head_sha, "bundle": bundle_head})

        if source_head_sha == bundle_head:
            logger.info(f"CloudFront and Source HEAD match ({source_head_sha}). Skipping clone and bundle.")
            sentinel.touch()
            return
        else:
            logger.info(
                f"Bundle is stale (bundle={bundle_head!r}, remote={source_head_sha!r}). "
                "Seeding from existing bundle then pulling updates."
            )
            logger.info(f"Downloading existing bundle for {dataset_name!r}...")
            download_bundle(dataset_name, bundle_path)

            if not (target_dir / ".git").exists() and not (target_dir / ".kart").exists():
                clone_from_bundle(bundle_path, target_dir)

            run_command(["git", "remote", "set-url", "origin", dataset_source], cwd=str(target_dir))
            run_command(["git", "pull"], cwd=str(target_dir))

    if (target_dir / ".git").exists() or (target_dir / ".kart").exists():
        if should_pull(target_dir):
            run_command(["git", "remote", "set-url", "origin", dataset_source], cwd=str(target_dir))
            logger.info("Attempting 'git pull'.")
            run_command(["git", "pull"], cwd=str(target_dir))
    else:
        run_command(["kart", "clone", dataset_source, str(target_dir), "--no-checkout"])

    run_command(["git", "-C", str(target_dir), "bundle", "create", str(bundle_path), "--all"])

    head_sha = run_command(["git", "rev-parse", "HEAD"], cwd=str(target_dir)).strip()

    # Upload to S3 via aws s3 cp
    s3_url = env_bundle_s3_url()

    logger.info(f"Uploading bundle to {s3_url}...")
    run_command(["aws", "s3", "cp", str(bundle_path), f"{s3_url}{dataset_name}.bundle"])

    kart_dataset_id = get_kart_dataset_id(target_dir)
    per_commit_dir = WORKING_EXPORTS_DIR / dataset_name
    per_commit_dir.mkdir(parents=True, exist_ok=True)

    def export_commit(sha: str) -> bool:
        s3_key = f"{s3_url}{dataset_name}/{sha}.json.gz"
        result = run_command(["aws", "s3", "ls", s3_key], check_error=False)
        if result.strip():
            return False
        json_export = str(per_commit_dir / f"{sha}.json")
        run_command(
            ["kart", "export", "--overwrite", "--ref", sha, kart_dataset_id, json_export],
            cwd=str(target_dir),
        )
        run_command(["gzip", "-f", "-9", json_export])
        run_command(["aws", "s3", "cp", f"{json_export}.gz", s3_key])
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
    with log_context(action="bundle", dataset=sys.argv[1]):
        bundle_dataset(sys.argv[1])
