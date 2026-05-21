import time
import urllib.error
import urllib.request
from pathlib import Path

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from ..command import run_command
from ..config import (
    SOURCE_DIR,
    get_bundle_url,
    get_dataset_name,
    get_datasets,
    get_s3_bundle_uri,
)

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
    return not time.time() - last_pull_time < 24 * 3600


def fetch_bundle_head(dataset_name: str) -> str | None:
    """
    git bundles start with the ref list in plain text followed by the pack files

    Extract the current HEAD sha if it exists from the first 128KB of the bundle
    """
    remote_bundle_url = get_bundle_url(dataset_name)
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


def make_bundle_asset(dataset_source: str):
    dataset_name = get_dataset_name(dataset_source)
    print(f"{dataset_source} -> {dataset_name}")

    @asset(name=f"bundle_{dataset_name}", group_name="kart")
    def _bundle_asset(context: AssetExecutionContext):
        SOURCE_DIR.mkdir(parents=True, exist_ok=True)
        target_dir = SOURCE_DIR / dataset_name
        bundle_path = SOURCE_DIR / f"{dataset_name}.bundle"

        bundle_head = fetch_bundle_head(dataset_name)

        # Skip bundle creation if the current bundle and remote are the same hash
        if bundle_head:
            ls_remote_cmd = ["git", "ls-remote", dataset_source, "HEAD"]
            ls_remote_out = run_command(context, ls_remote_cmd).strip()
            source_head_sha = ls_remote_out.split()[0] if ls_remote_out else None

            if source_head_sha == bundle_head:
                context.log.info(f"CloudFront and Source HEAD match ({source_head_sha}). Skipping clone and bundle.")
                return MaterializeResult(
                    metadata={
                        "skipped": MetadataValue.bool(True),
                        "head_sha": MetadataValue.text(source_head_sha),
                    }
                )

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

        head_sha = run_command(context, ["git", "rev-parse", "HEAD"], cwd=str(target_dir)).strip()

        # Upload to S3 via aws s3 cp
        s3_uri = get_s3_bundle_uri()

        context.log.info(f"Uploading bundle to {s3_uri}...")
        aws_cp_bundle = [
            "aws",
            "s3",
            "cp",
            str(bundle_path),
            f"{s3_uri}{dataset_name}.bundle",
        ]
        run_command(context, aws_cp_bundle)

        context.log.info(f"Successfully uploaded {dataset_name}.bundle")

        return MaterializeResult(
            metadata={
                "head_sha": MetadataValue.text(head_sha),
            }
        )

    return _bundle_asset


bundle_assets = [make_bundle_asset(t) for t in get_datasets()]


@asset(deps=bundle_assets)
def bundle_all(context: AssetExecutionContext):
    """Wait for all bundle assets to be created and checked."""
    context.log.info("All bundles successfully processed.")
    return MaterializeResult(metadata={"status": MetadataValue.text("ok")})
