import hashlib
from pathlib import Path

HASH_SUFFIX = ".hash"


def sha256_file(path: Path) -> str:
    """Streams `path` through SHA-256 so large files don't need to fit in memory."""
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_hash_sidecar(path: Path) -> str:
    """Hashes `path` and writes the digest to a `path.name + ".hash"` sidecar, returning it.

    Written right after `path` is produced, so a reader never has to re-hash a (potentially
    large) file just to check whether it changed since the last time it was read.
    """
    digest = sha256_file(path)
    path.with_name(path.name + HASH_SUFFIX).write_text(digest)
    return digest


def read_hash_sidecar(path: Path) -> str | None:
    """Reads the digest `write_hash_sidecar` left for `path`, or None if there isn't one."""
    hash_file = path.with_name(path.name + HASH_SUFFIX)
    if not hash_file.exists():
        return None
    return hash_file.read_text().strip()
