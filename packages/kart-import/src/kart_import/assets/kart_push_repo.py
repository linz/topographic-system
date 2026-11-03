"""Push a built target repo (`data/output/<repo>`) to its GitHub remote.

The combined repo produced by `kart_import_repo` is a git repo whose commits carry the Kart dataset trees.
By default, we push to a single branch named after the latest release (e.g. `feat/release66`).
The branch contains the entire import history, ready to PR into `master`.
Pass `--force` to force-push.
Pass `--master` to push to `master`.
Combine for destructive full reload.

Pushed one commit at a time, each release's marker commit also tagged `release/<id>`: a
whole-history push exceeds GitHub's 2GB pack limit, and so -- for a release with enough new
or changed rows -- can a single release's own import commit (see `kart_import.git.linearise`,
which splits one of those into several commits before it ever gets here). Pushing commit by
commit, rather than release by release, keeps every single push under that limit regardless,
and commits already on the remote make a retry resumable.
"""

import logging
import subprocess
import time
from pathlib import Path

from kart_import.log import log_context

from ..command import run_command
from ..config import OUTPUT_DIR, get_releases, get_repo_remote
from ..env import env_push_force, env_push_to_master

logger = logging.getLogger("kart_import")

PUSH_ATTEMPTS = 4
PUSH_RETRY_PAUSE_SECONDS = 10
"""GitHub intermittently rejects one of many quick pushes to a branch (`remote: fatal error in
commit_refs`), so a failed push pauses and pushes the same commit again rather than ending the run."""


def push_with_retry(git_command: list[str], repo_dir: Path) -> None:
    for attempt in range(1, PUSH_ATTEMPTS + 1):
        try:
            run_command(git_command, cwd=repo_dir)
            return
        except subprocess.CalledProcessError:
            if attempt == PUSH_ATTEMPTS:
                raise
            logger.warning("push failed, retrying", extra={"attempt": attempt, "of": PUSH_ATTEMPTS})
            time.sleep(PUSH_RETRY_PAUSE_SECONDS)


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
        if word == "release:" and release.isdigit():
            checkpoints.append((int(release), sha))

    if not checkpoints:
        raise ValueError(f"No release markers found in {repo_dir}; nothing to push")
    return checkpoints


def all_commits(repo_dir: Path) -> list[str]:
    """Every commit sha on the built branch, oldest first -- the unit `push_repo` pushes one at a time."""
    return run_command(["git", "rev-list", "--reverse", "HEAD"], cwd=repo_dir).split()


def push_repo(repo_name: str, to_master: bool = False, force: bool = False) -> str:
    repo_dir = OUTPUT_DIR / repo_name
    if not (repo_dir / ".git").exists():
        raise RuntimeError(f"Target repo not built (run kart_import_repo first): {repo_dir}")

    url = get_repo_remote(repo_name)
    ref = "master" if to_master else release_branch()

    run_command(["git", "remote", "remove", "origin"], cwd=repo_dir, allow_error="No such remote")
    run_command(["git", "remote", "add", "origin", url], cwd=repo_dir)

    checkpoints = release_checkpoints(repo_dir)
    release_at = dict((sha, release_id) for release_id, sha in checkpoints)
    final_release = checkpoints[-1][0]
    commits = all_commits(repo_dir)

    logger.info(
        "pushing target repo",
        extra={
            "repo": repo_name,
            "url": url,
            "ref": ref,
            "mode": "master" if to_master else "branch",
            "commits": len(commits),
            "releases": len(checkpoints),
        },
    )

    for sha in commits:
        git_command = ["git", "push"]
        if force:
            git_command.append("--force")
        refspecs = [f"{sha}:refs/heads/{ref}"]

        release_id = release_at.get(sha)
        if release_id is not None:
            tag = release_tag(release_id)
            run_command(["git", "tag", "--force", tag, sha], cwd=repo_dir)
            refspecs.append(f"refs/tags/{tag}")

        # Branch (and tag, at a release marker) in one round trip; a commit already on the
        # remote is a no-op, not a re-send.
        git_command.extend(["origin", *refspecs])
        push_with_retry(git_command, repo_dir)

        if release_id is not None:
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
