"""Push a built target repo (`data/output/<repo>`) to its GitHub remote.

The combined repo produced by `kart_import_repo` is a git repo whose commits carry the Kart dataset trees.
By default, we push to a single branch named after the latest release (e.g. `feat/release66`).
The branch contains the entire import history, ready to PR into `master`.
Pass `--force` to force-push.
Pass `--master` to push to `master`.
Combine for destructive full reload.

Pushed one release at a time, each tagged `release/<id>`: a whole-history push exceeds GitHub's
2GB pack limit, and releases already on the remote make a retry resumable.
"""

import logging
from pathlib import Path

from kart_import.log import log_context

from ..command import run_command
from ..config import OUTPUT_DIR, get_releases, get_repo_remote
from ..env import env_push_force, env_push_to_master

logger = logging.getLogger("kart_import")


def release_branch() -> str:
    releases = get_releases()
    return f"feat/release{releases[-1].id}" if releases else "import"


def release_tag(release_id: int) -> str:
    """Tag naming a release's last commit; lightweight so a rebuild tags identically."""
    return f"release/{release_id}"


def release_checkpoints(repo_dir: Path) -> list[tuple[int, str]]:
    """Each release's marker commit, oldest first -- see `kart_import.git.linearise`."""
    log = run_command(["git", "log", "--reverse", "--format=%H %s"], cwd=repo_dir)

    checkpoints: list[tuple[int, str]] = []
    for line in log.splitlines():
        sha, _, subject = line.partition(" ")
        word, _, release = subject.partition(" ")
        if word == "release" and release.isdigit():
            checkpoints.append((int(release), sha))

    if not checkpoints:
        raise ValueError(f"No release markers found in {repo_dir}; nothing to push")
    return checkpoints


def push_repo(repo_name: str, to_master: bool = False, force: bool = False) -> str:
    repo_dir = OUTPUT_DIR / repo_name
    if not (repo_dir / ".git").exists():
        raise RuntimeError(f"Target repo not built (run kart_import_repo first): {repo_dir}")

    url = get_repo_remote(repo_name)
    ref = "master" if to_master else release_branch()

    run_command(["git", "remote", "remove", "origin"], cwd=repo_dir, allow_error="No such remote")
    run_command(["git", "remote", "add", "origin", url], cwd=repo_dir)

    checkpoints = release_checkpoints(repo_dir)
    final_release = checkpoints[-1][0]

    logger.info(
        "pushing target repo",
        extra={
            "repo": repo_name,
            "url": url,
            "ref": ref,
            "mode": "master" if to_master else "branch",
            "releases": len(checkpoints),
        },
    )

    for release_id, sha in checkpoints:
        tag = release_tag(release_id)
        run_command(["git", "tag", "--force", tag, sha], cwd=repo_dir)

        git_command = ["git", "push"]
        if force:
            git_command.append("--force")
        # Branch and tag in one round trip; an already-pushed release is a no-op, not a re-send.
        git_command.extend(["origin", f"{sha}:refs/heads/{ref}", f"refs/tags/{tag}"])
        run_command(git_command, cwd=repo_dir)

        logger.info("pushing chunk", extra={"push": release_id, "of": final_release})

    (repo_dir / ".pushed").write_text(f"{url} {ref}\n")
    logger.info("pushed", extra={"repo": repo_name, "ref": ref, "releases": len(checkpoints)})
    return ref


if __name__ == "__main__":
    import sys

    args = sys.argv[1:]
    to_master = "--master" in args or env_push_to_master()
    force = "--force" in args or env_push_force()
    positional = [a for a in args if not a.startswith("-")]
    if not positional:
        print("Usage: python -m kart_import.assets.kart_push_repo <repo_name> [--master] [--force]")
        sys.exit(1)
    with log_context(action="kart_push_repo", repo=positional[0]):
        push_repo(positional[0], to_master=to_master, force=force)
