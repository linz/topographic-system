import subprocess
from datetime import datetime
from pathlib import Path


def get_release_commit(repo_dir: Path, release_until: datetime | None) -> tuple[str, str] | None:
    """Finds the last commit before the given date. Returns (hash, iso_time)."""
    if not release_until:
        # For the latest release, use the latest commit
        cmd = ["git", "log", "-n", "1", "--pretty=format:%H|%cI"]
    else:
        cmd = ["git", "log", "-n", "1", "--until", release_until.isoformat(), "--pretty=format:%H|%cI"]

    result = subprocess.run(cmd, cwd=str(repo_dir), capture_output=True, text=True)
    if result.returncode == 0 and result.stdout.strip():
        parts = result.stdout.strip().split("|")
        if len(parts) == 2:
            return parts[0], parts[1]
    return None
