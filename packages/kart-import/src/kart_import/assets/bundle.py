from pathlib import Path
import time

from ..config import get_datasets, get_dataset_name, SOURCE_DIR, get_bundle_url, get_s3_bundle_uri
from dagster import asset, AssetExecutionContext
from ..command import run_command

import urllib.request
import urllib.error

"""
Take datasets from the linz data service, bundle them into a git .bundle
then store them into AWS S3 for fast access
"""

def should_pull(target_dir: Path):
    """
    Limit pulls to once per day, so not to overload kart
    """
    fetch_head = target_dir / ".kart" / "FETCH_HEAD"
    if not fetch_head.exists():
        fetch_head = target_dir / ".git" / "FETCH_HEAD"

    if not fetch_head.exists():
        return True

    last_pull_time = fetch_head.stat().st_mtime
    if time.time() - last_pull_time < 24 * 3600:
        return False

    return True

def fetch_bundle_head(dataset_name: str) -> str:
    remote_head_url = get_bundle_url(dataset_name) + ".head"
    try:
        req = urllib.request.Request(remote_head_url)
        with urllib.request.urlopen(req) as response:
            return response.read().decode("utf-8").strip()
    except urllib.error.URLError:
        return None


def make_bundle_asset(dataset_source: str):
    dataset_name = get_dataset_name(dataset_source)
    print(f"{dataset_source} -> {dataset_name}")

    @asset(name=f"bundle_{dataset_name}", group_name="kart")
    def _bundle_asset(context: AssetExecutionContext):
        SOURCE_DIR.mkdir(parents=True, exist_ok=True)
        target_dir = SOURCE_DIR / dataset_name
        bundle_path = SOURCE_DIR / f"{dataset_name}.bundle"
        sidecar_path = SOURCE_DIR / f"{dataset_name}.bundle.head"

        bundle_head = fetch_bundle_head(dataset_name)

        # Skip bundle creation if the current bundle and remote are the same hash
        if bundle_head:
            ls_remote_cmd = ["git", "ls-remote", dataset_source, "HEAD"]
            ls_remote_out = run_command(context, ls_remote_cmd).strip()
            source_head_sha = ls_remote_out.split()[0] if ls_remote_out else None

            if source_head_sha == bundle_head:
                context.log.info(f"CloudFront and Source HEAD match ({source_head_sha}). Skipping clone and bundle.")
                return None

        if (target_dir / ".git").exists() or (target_dir / ".kart").exists():
            if should_pull(target_dir):
                context.log.info("Attempting 'git pull'.")
                cmd = ["git", "pull"]
                run_command(context, cmd, cwd=str(target_dir))
        else:
            cmd = [
                "git",
                "clone",
                f"{dataset_source}",
                str(target_dir),
                "--no-checkout",
            ]
            run_command(context, cmd)

        cmd = [
            "git",
            "-C",
            str(target_dir),
            "bundle",
            "create",
            str(bundle_path),
            "--all",
        ]
        run_command(context, cmd)

        head_cmd = ["git", "-C", str(target_dir), "rev-parse", "HEAD"]
        head_sha = run_command(context, head_cmd).strip()
        sidecar_path.write_text(head_sha)

        # Upload to S3 via aws s3 cp
        s3_uri = get_s3_bundle_uri()
        if not s3_uri.endswith("/"):
            s3_uri += "/"
        context.log.info(f"Uploading bundle and head file to {s3_uri}...")
        
        # Copy the bundle file
        aws_cp_bundle = ["aws", "s3", "cp", str(bundle_path), f"{s3_uri}{dataset_name}.bundle"]
        run_command(context, aws_cp_bundle)
        
        # Copy the head file
        aws_cp_head = ["aws", "s3", "cp", str(sidecar_path), f"{s3_uri}{dataset_name}.bundle.head"]
        run_command(context, aws_cp_head)
        
        context.log.info(f"Successfully uploaded {dataset_name}.bundle and {dataset_name}.bundle.head")

        return str(bundle_path)

    return _bundle_asset


bundle_assets = [make_bundle_asset(t) for t in get_datasets()]

@asset(deps=bundle_assets)
def bundle_all(context: AssetExecutionContext):
    """Wait for all bundle assets to be created and checked."""
    context.log.info("All bundles successfully processed.")
