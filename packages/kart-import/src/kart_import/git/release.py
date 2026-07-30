import subprocess
from datetime import datetime
from pathlib import Path

from .kart import source_ref


def get_release_commit(repo_dir: Path, release_until: datetime | None) -> tuple[str, str] | None:
    """Finds the last commit before the given date. Returns (hash, iso_time).

    Resolves against the source's remote-tracking tip (``source_ref``) rather than the local
    ``HEAD``, so a stale local branch can't make us pick an outdated commit.
    """
    ref = source_ref(repo_dir)
    if not release_until:
        # For the latest release, use the latest commit
        cmd = ["git", "log", "-n", "1", "--pretty=format:%H|%cI", ref]
    else:
        cmd = ["git", "log", "-n", "1", "--until", release_until.isoformat(), "--pretty=format:%H|%cI", ref]

    result = subprocess.run(cmd, cwd=str(repo_dir), capture_output=True, text=True)
    if result.returncode == 0 and result.stdout.strip():
        parts = result.stdout.strip().split("|")
        if len(parts) == 2:
            return parts[0], parts[1]
    return None
