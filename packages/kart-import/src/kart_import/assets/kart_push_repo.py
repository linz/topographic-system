"""Push a built target repo (`data/output/<repo>`) to its GitHub remote.

The combined repo produced by `kart_import_repo` is a git repo whose commits carry the Kart dataset trees.
By default, we push to a single branch named after the latest release (e.g. `feat/release66`).
The branch contains the entire import history, ready to PR into `master`.
Pass `--force` to force-push.
Pass `--master` to push to `master`.
Combine for destructive full reload.
"""

import logging

from kart_import.log import log_context

from ..command import run_command
from ..config import OUTPUT_DIR, get_releases, get_repo_remote

logger = logging.getLogger("kart_import")


def release_branch() -> str:
    releases = get_releases()
    return f"feat/release{releases[-1].id}" if releases else "import"


def push_repo(repo_name: str, to_master: bool = False, force: bool = False) -> str:
    repo_dir = OUTPUT_DIR / repo_name
    if not (repo_dir / ".git").exists():
        raise RuntimeError(f"Target repo not built (run kart_import_repo first): {repo_dir}")

    url = get_repo_remote(repo_name)
    ref = "master" if to_master else release_branch()

    run_command(["git", "remote", "remove", "origin"], cwd=repo_dir, allow_error="No such remote")
    run_command(["git", "remote", "add", "origin", url], cwd=repo_dir)

    logger.info(
        "pushing target repo",
        extra={"repo": repo_name, "url": url, "ref": ref, "mode": "master" if to_master else "branch"},
    )

    git_command = ["git", "push"]
    if force:
        git_command.append("--force")
    git_command.extend(["origin", f"HEAD:refs/heads/{ref}"])

    run_command(git_command, cwd=repo_dir)

    (repo_dir / ".pushed").write_text(f"{url} {ref}\n")
    logger.info("pushed", extra={"repo": repo_name, "ref": ref})
    return ref


if __name__ == "__main__":
    import sys

    args = sys.argv[1:]
    to_master = "--master" in args
    force = "--force" in args
    positional = [a for a in args if not a.startswith("-")]
    if not positional:
        print("Usage: python -m kart_import.assets.kart_push_repo <repo_name> [--master] [--force]")
        sys.exit(1)
    with log_context(action="kart_push_repo", repo=positional[0]):
        push_repo(positional[0], to_master=to_master, force=force)
