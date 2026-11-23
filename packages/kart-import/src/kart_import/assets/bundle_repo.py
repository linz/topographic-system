"""Pack a built target repo (`data/output/<repo>/`) into a single git bundle file.

The bundle holds the repo's whole history, so it can be downloaded and cloned directly:

    kart clone <repo>.bundle <repo>
"""

import logging

from kart_import.log import log_context

from ..command import run_command
from ..config import BUNDLE_DIR, OUTPUT_DIR

logger = logging.getLogger("kart_import")


def bundle_repo(repo_name: str) -> str:
    repo_dir = OUTPUT_DIR / repo_name
    bundle_file = (BUNDLE_DIR / f"{repo_name}.bundle").resolve()
    BUNDLE_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Creating bundle", extra={"repo": repo_name, "bundle": str(bundle_file)})
    run_command(["git", "bundle", "create", str(bundle_file), "--all"], cwd=str(repo_dir))
    # Confirm the bundle is complete and readable before anyone downloads it
    run_command(["git", "bundle", "verify", str(bundle_file)], cwd=str(repo_dir))

    logger.info(
        "Bundle created", extra={"repo": repo_name, "bundle": str(bundle_file), "bytes": bundle_file.stat().st_size}
    )
    return str(bundle_file)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m kart_import.assets.bundle_repo <repo_name>")
        sys.exit(1)
    with log_context(action="bundle_repo", repo=sys.argv[1]):
        bundle_repo(sys.argv[1])
