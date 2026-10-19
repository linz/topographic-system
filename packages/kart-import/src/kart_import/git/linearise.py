"""Replay disjoint theme histories into one chronologically ordered linear history.

Each theme commit only ever touches its own root-level subtree, so combining themes needs
no merging: `git fast-import` restacks each commit's subtree by SHA. `git cherry-pick`
would three-way merge and rewrite the index per commit -- work proportional to the whole
repo -- and would stamp a new committer date, so rebuilds would not be reproducible.

Each release is closed with an empty `release: <id>` commit, so the interleaved history reads
by release rather than by whichever theme happened to sort last.

Only `M` lines are emitted, so deletions are never replayed. That is safe because a theme
history never drops a root-level entry: each theme repo is rebuilt from scratch and every
release imported into the one dataset named after the theme.

A `kart import --replace-existing` commit is a full snapshot, so a release with a lot of new
or changed rows can still land as one very large commit even though imports happen one release
at a time. `kart_push_repo` pushes one commit at a time precisely so a push never has to carry
more than one commit's worth of new objects; if a single commit's own new data is too big for
that, no amount of push-side chunking helps. So here, before writing a commit, we measure the
real pack bytes it would add (`_packed_size`, the same object-graph diff a push would compute)
and, if that's over `threshold`, replay it as several consecutive commits instead of one --
each a partial step toward the same final tree, split at whatever path depth makes each part
fit (`_collect_units`, `_pack_groups`). This needs no knowledge of a dataset's internal layout:
`git fast-import`'s `M` command accepts a full nested path just as well as a root-level one.
"""

import logging
import subprocess
from pathlib import Path

from ..env import env_commit_split_bytes

logger = logging.getLogger("kart_import")


class TreeCollision(RuntimeError):
    """Two themes claim the same root-level path, so their histories are not disjoint."""


def _parse_commit(body: bytes) -> tuple[str, bytes, bytes, bytes]:
    """Split a raw commit object into (tree sha, author line, committer line, message)."""
    header, _, message = body.partition(b"\n\n")
    tree = author = committer = None
    for line in header.split(b"\n"):
        if line.startswith(b"tree "):
            tree = line[5:].decode()
        elif line.startswith(b"author "):
            author = line[7:]
        elif line.startswith(b"committer "):
            committer = line[10:]
    if tree is None or author is None or committer is None:
        raise ValueError(f"Malformed commit object: {header!r}")
    return tree, author, committer, message


def _parse_tree(body: bytes) -> list[tuple[str, str, str]]:
    """Root-level entries of a raw tree object as (mode, name, sha).

    Modes are zero-padded: git stores a tree as `40000`, fast-import wants `040000`.
    """
    entries: list[tuple[str, str, str]] = []
    offset = 0
    while offset < len(body):
        space = body.index(b" ", offset)
        nul = body.index(b"\x00", space)
        mode = body[offset:space].decode().zfill(6)
        name = body[space + 1 : nul].decode()
        sha = body[nul + 1 : nul + 21].hex()
        entries.append((mode, name, sha))
        offset = nul + 21
    return entries


class _ObjectReader:
    """A single long-lived `git cat-file --batch`, so the whole replay costs one process."""

    def __init__(self, repo_dir: Path | str):
        self._proc = subprocess.Popen(
            ["git", "cat-file", "--batch"],
            cwd=str(repo_dir),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )

    def read(self, ref: str, expected: str) -> bytes:
        assert self._proc.stdin and self._proc.stdout
        self._proc.stdin.write(f"{ref}\n".encode())
        self._proc.stdin.flush()
        header = self._proc.stdout.readline().split()
        if len(header) != 3:
            raise ValueError(f"git cat-file could not read {ref}: {b' '.join(header)!r}")
        kind, size = header[1].decode(), int(header[2])
        if kind != expected:
            raise ValueError(f"Expected {ref} to be a {expected}, got {kind}")
        body = self._proc.stdout.read(size)
        self._proc.stdout.read(1)  # trailing newline
        return body

    def close(self) -> None:
        assert self._proc.stdin
        self._proc.stdin.close()
        self._proc.wait()


def _check_disjoint(owners: dict[str, tuple[str, str]], name: str, mode: str, sha: str, theme: str) -> None:
    """Reject the overlap cherry-pick would have raised as a merge conflict.

    A root-level tree is a theme's dataset and only that theme may write it. A root-level
    blob is shared repo metadata, writable by any theme that agrees on its content.
    """
    previous = owners.get(name)
    if previous is None or previous[0] == theme:
        owners[name] = (theme, sha)
        return

    owner, owner_sha = previous
    if mode == "040000":
        raise TreeCollision(f"Themes {owner!r} and {theme!r} both write the dataset {name!r}")
    if sha != owner_sha:
        raise TreeCollision(f"Themes {owner!r} and {theme!r} disagree on shared file {name!r}")


def _release(message: bytes) -> bytes | None:
    """Release id out of a `kart_import_theme` subject: "feat(<theme>): import release <id>"."""
    subject = message.partition(b"\n")[0]
    _, marker, release = subject.rpartition(b" release ")
    return release if marker and release.isdigit() else None


def _write_commit(stream, branch: str, mark: int, author: bytes, committer: bytes, message: bytes) -> None:
    """Commit header up to (not including) its file operations."""
    stream.write(f"commit refs/heads/{branch}\n".encode())
    stream.write(f"mark :{mark}\n".encode())
    stream.write(b"author " + author + b"\n")
    stream.write(b"committer " + committer + b"\n")
    stream.write(f"data {len(message)}\n".encode())
    stream.write(message)
    if mark > 1:
        stream.write(f"from :{mark - 1}\n".encode())


def _tag_part(message: bytes, index: int, total: int) -> bytes:
    """Mark one piece of a commit split across several fast-import commits for size.

    Only touches the displayed subject -- `_release()` keys off the untouched original message,
    so a split import commit still closes its release exactly where it would have unsplit.
    """
    subject, sep, rest = message.partition(b"\n")
    return subject + f" (part {index}/{total})".encode() + sep + rest


_Unit = tuple[int, str, str, str]  # (new pack bytes, mode, path, sha)
_Entry = tuple[str, str, str]  # (mode, path, sha)


def _packed_size(repo_dir: Path | str, old_sha: str | None, new_sha: str) -> int:
    """Real pack bytes a push would need for `new_sha`'s objects not already reachable from
    `old_sha` -- i.e. exactly what `kart_push_repo` would have to send for this change, not a
    heuristic over raw blob sizes (delta compression moves that number a lot)."""
    revs = f"{new_sha}\n" + (f"^{old_sha}\n" if old_sha else "")
    result = subprocess.run(
        ["git", "pack-objects", "--stdout", "--revs", "-q"],
        cwd=str(repo_dir),
        input=revs.encode(),
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=True,
    )
    return len(result.stdout)


def _collect_units(
    repo_dir: Path | str,
    reader: _ObjectReader,
    path: str,
    mode: str,
    old_sha: str | None,
    new_sha: str,
    threshold: int,
) -> list[_Unit]:
    """Break a changed entry down only as far as needed to get each piece under `threshold`.

    Stops as soon as a (sub)tree already fits, so a release that's mostly small changes with
    one big new directory only pays the recursion cost where the data actually is. A single
    blob over threshold can't be split further and comes back as its own oversized unit.
    """
    if old_sha == new_sha:
        return [(0, mode, path, new_sha)]

    size = _packed_size(repo_dir, old_sha, new_sha)
    if size <= threshold or mode != "040000":
        return [(size, mode, path, new_sha)]

    old_children = {name: sha for _, name, sha in _parse_tree(reader.read(old_sha, "tree"))} if old_sha else {}
    units: list[_Unit] = []
    for child_mode, child_name, child_sha in _parse_tree(reader.read(new_sha, "tree")):
        child_old = old_children.get(child_name)
        if child_old == child_sha:
            continue
        units.extend(_collect_units(repo_dir, reader, f"{path}/{child_name}", child_mode, child_old, child_sha, threshold))
    return units


def _pack_groups(units: list[_Unit], threshold: int) -> list[list[_Entry]]:
    """Greedily batch units, in their original order, into same-release commits worth of `M`
    lines -- each group at or just over `threshold` only because its own last unit pushed it
    there, never because two groups were merged that didn't need to be."""
    groups: list[list[_Entry]] = []
    current: list[_Entry] = []
    current_size = 0
    for size, mode, path, sha in units:
        if current and current_size + size > threshold:
            groups.append(current)
            current, current_size = [], 0
        current.append((mode, path, sha))
        current_size += size
    if current:
        groups.append(current)
    return groups or [[]]


def _plan_commit(
    repo_dir: Path | str, reader: _ObjectReader, entries: list[_Entry], previous: dict[str, str], threshold: int
) -> list[list[_Entry]]:
    """`entries`' `M` lines, batched into one or more same-release commits worth of writes."""
    units: list[_Unit] = []
    for mode, name, sha in entries:
        units.extend(_collect_units(repo_dir, reader, name, mode, previous.get(name), sha, threshold))
    return _pack_groups(units, threshold)


def linearise(
    repo_dir: Path | str, commits: list[tuple[str, str]], branch: str = "master", threshold: int | None = None
) -> None:
    """Replay `commits` onto `branch` as a linear history, preserving each commit's tree.

    :param repo_dir: repo holding every theme's objects (fetched from their bundles)
    :param commits: (theme name, commit sha) in the order they should appear, oldest first
    :param branch: branch to build; must not already exist
    :param threshold: max new pack bytes per resulting commit before it's split (default from
        `KART_COMMIT_SPLIT_BYTES`, see `_collect_units`)
    """
    if threshold is None:
        threshold = env_commit_split_bytes()
    if not commits:
        raise ValueError("No commits to linearise")

    # The first commit has no `from`: fast-import would reset an existing branch and orphan it.
    existing = subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", f"refs/heads/{branch}"],
        cwd=str(repo_dir),
        capture_output=True,
    )
    if existing.returncode == 0:
        raise ValueError(f"Branch {branch!r} already exists in {repo_dir}; replaying onto it would discard its history")

    reader = _ObjectReader(repo_dir)
    # --done makes a truncated stream an error rather than a silently short history.
    importer = subprocess.Popen(
        ["git", "fast-import", "--quiet", "--done"],
        cwd=str(repo_dir),
        stdin=subprocess.PIPE,
    )
    assert importer.stdin
    stream = importer.stdin
    owners: dict[str, tuple[str, str]] = {}

    mark = 0
    release: bytes | None = None
    dates: tuple[bytes, bytes] | None = None

    try:
        for theme, sha in commits:
            tree_sha, author, committer, message = _parse_commit(reader.read(sha, "commit"))
            entries = _parse_tree(reader.read(tree_sha, "tree"))

            found = _release(message)
            if found and release is not None and found != release:
                mark += 1
                assert dates
                _write_commit(stream, branch, mark, *dates, b"release: " + release + b"\n")
                stream.write(b"\n")
            if found:
                release = found
            dates = (author, committer)

            previous = {name: prior_sha for name, (_, prior_sha) in owners.items()}
            for mode, name, entry_sha in entries:
                _check_disjoint(owners, name, mode, entry_sha, theme)

            groups = _plan_commit(repo_dir, reader, entries, previous, threshold)
            if len(groups) > 1:
                logger.info(
                    "splitting oversized commit",
                    extra={"theme": theme, "sha": sha, "parts": len(groups), "threshold": threshold},
                )
            for index, group in enumerate(groups, start=1):
                mark += 1
                part_message = message if len(groups) == 1 else _tag_part(message, index, len(groups))
                _write_commit(stream, branch, mark, author, committer, part_message)
                for mode, path, entry_sha in group:
                    stream.write(f"M {mode} {entry_sha} {path}\n".encode())
                stream.write(b"\n")

        if release is not None:
            mark += 1
            assert dates
            _write_commit(stream, branch, mark, *dates, b"release: " + release + b"\n")
            stream.write(b"\n")

        stream.write(b"done\n")
        stream.close()
    except Exception:
        importer.kill()
        importer.wait()
        raise
    finally:
        reader.close()

    if importer.wait() != 0:
        raise subprocess.CalledProcessError(importer.returncode, ["git", "fast-import"])
