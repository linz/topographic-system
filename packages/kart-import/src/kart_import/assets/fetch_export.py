import json
import logging
import shutil
import time
import urllib.error
import urllib.request
from pathlib import Path

from ..command import run_command
from ..config import get_releases, get_source_entry, get_source_repo_name
from ..env import env_export_url
from ..export_paths import (
    entry_exported_sentinel,
    export_entry_dir,
    load_releases_map,
    release_config_hash,
    releases_map_file,
    schema_sidecar_file,
    source_commit_hash,
)
from ..log import log_context
from ..thread import run_in_thread_pool

logger = logging.getLogger("kart_import")

NETWORK_RETRIES = 3
HTTP_TIMEOUT_SECONDS = 60


def _http_download(url: str, dest: Path) -> None:
    """Stream an HTTP(S) URL to `dest`, retrying transient network/5xx errors."""
    for attempt in range(NETWORK_RETRIES + 1):
        try:
            with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT_SECONDS) as resp, open(dest, "wb") as out:
                shutil.copyfileobj(resp, out)
            return
        except urllib.error.HTTPError as exc:
            if exc.code < 500 or attempt == NETWORK_RETRIES:
                raise
            logger.warning("transient HTTP error, retrying", extra={"url": url, "status": exc.code, "attempt": attempt + 1})
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            if attempt == NETWORK_RETRIES:
                raise
            logger.warning("transient download error, retrying", extra={"url": url, "error": str(exc), "attempt": attempt + 1})
        time.sleep(2**attempt)


def download_file(url: str, dest: Path) -> None:
    """Download `url` (`https://`, `http://`, or `s3://`) atomically to `dest`."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(f".{dest.name}.tmp")
    try:
        if url.startswith("s3://"):
            run_command(["aws", "s3", "cp", url, str(tmp)], retries=NETWORK_RETRIES)
        elif url.startswith(("https://", "http://")):
            _http_download(url, tmp)
        else:
            raise ValueError(f"Unsupported export URL scheme: {url!r} (expected https://, http://, or s3://)")
        tmp.replace(dest)
    except BaseException:
        if tmp.exists():
            tmp.unlink()
        raise


def _validate_releases_hashes(entry_name: str, meta: dict) -> None:
    """Ensure the fetched `<entry>.releases.json` was built with the same `topo50_release.yml`
    (`config`) and pipeline git commit (`commit`) as this checkout."""
    expected_config = release_config_hash()
    remote_config = meta.get("config")
    if remote_config != expected_config:
        raise ValueError(
            f"Remote releases.json for {entry_name!r} has config hash {remote_config!r}, "
            f"expected {expected_config!r} (topo50_release.yml differs)"
        )

    expected_commit = source_commit_hash()
    remote_commit = meta.get("commit")
    if remote_commit != expected_commit:
        raise ValueError(
            f"Remote releases.json for {entry_name!r} has commit hash {remote_commit!r}, "
            f"expected {expected_commit!r} (pipeline git commit differs)"
        )


def fetch_entry(entry_name: str, base_url: str | None = None) -> str:
    """Fetch `<entry_name>.releases.json` from `base_url` (`KART_EXPORT_URL`), verify its
    `config` and `commit` hashes match ours, and download only the per-commit exports (and schema
    sidecars) needed by the configured releases that are not already present locally."""
    resolved_url = base_url if base_url is not None else env_export_url()
    if not resolved_url:
        raise ValueError("KART_EXPORT_URL must be set to fetch remote exports")
    if not resolved_url.endswith("/"):
        resolved_url += "/"

    entry = get_source_entry(entry_name)
    repo_name = get_source_repo_name(entry)
    output_dir = export_entry_dir(entry_name)
    output_dir.mkdir(parents=True, exist_ok=True)

    releases_file = releases_map_file(output_dir, entry_name)
    staged_releases = releases_file.with_name(f".{releases_file.name}.staged")
    try:
        download_file(f"{resolved_url}{repo_name}/{releases_file.name}", staged_releases)
        meta = json.loads(staged_releases.read_text())
        _validate_releases_hashes(entry_name, meta)
        staged_releases.replace(releases_file)
    except BaseException:
        if staged_releases.exists():
            staged_releases.unlink()
        raise

    releases_map = load_releases_map(output_dir, entry_name)
    needed: list[str] = []
    seen: set[str] = set()
    for release in get_releases():
        info = releases_map.get(release.id)
        if not info or info.get("location") is None:
            continue
        location = info["location"]
        sidecar = schema_sidecar_file(output_dir / location).name
        for filename in (location, sidecar):
            if filename not in seen:
                seen.add(filename)
                needed.append(filename)

    missing = [filename for filename in needed if not (output_dir / filename).exists()]
    if missing:
        run_in_thread_pool(
            func=lambda filename: download_file(f"{resolved_url}{repo_name}/{filename}", output_dir / filename),
            items=missing,
            thread_count=4,
        )

    sentinel = entry_exported_sentinel(entry_name)
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    sentinel.touch()

    logger.info(
        "fetch_export",
        extra={
            "entry": entry_name,
            "fetched": len(missing),
            "cached": len(needed) - len(missing),
            "url": resolved_url,
        },
    )
    return str(output_dir)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m kart_import.assets.fetch_export <entry_name>")
        sys.exit(1)
    with log_context(action="fetch_export", entry=sys.argv[1]):
        fetch_entry(sys.argv[1])

