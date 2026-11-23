import functools
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .command import run_command
from .config import (
    CONFIG_DIR_RELEASE,
    EXPORT_DIR,
    EXPORT_SUFFIX,
    THEME_SUFFIX,
    WORKING_THEME_DIR,
    get_source_entry,
    get_source_repo_name,
)


def release_config_hash() -> str:
    """A hash of `config/topo50_release.yml`'s current content, embedded in every release map
    (see `write_releases_map`) so a release definition change (a cutoff date edited, a release
    added) is caught when fetching remote exports."""
    return hashlib.sha256(CONFIG_DIR_RELEASE.read_bytes()).hexdigest()


@functools.cache
def source_commit_hash() -> str:
    """The git commit of the pipeline's own source code (not the data being exported), embedded
    in every release map (see `write_releases_map`) so it's traceable which version of the
    pipeline produced a given export. Cached: it can't change within a run."""
    return run_command(["git", "rev-parse", "HEAD"], cwd=str(Path(__file__).resolve().parent)).strip()


def export_filename(stem: str) -> str:
    """The filename for a per-commit source export (see EXPORT_FORMAT); every reader/writer of
    an export, in Python or the Snakefile, builds the name through this rather than appending
    EXPORT_SUFFIX itself."""
    return f"{stem}{EXPORT_SUFFIX}"


def export_entry_dir(name: str) -> Path:
    """Where `name`'s (a dataset or lookup) per-commit exports, schema sidecars and release map
    live: one directory per source repo under EXPORT_DIR, shared with every other dataset/lookup
    sourced from the same repo. Filenames within are prefixed with `name` (see
    `entry_export_filename`/`entry_export_files`) to avoid colliding with siblings there."""
    return EXPORT_DIR / get_source_repo_name(get_source_entry(name))


def entry_export_filename(name: str, stem: str) -> str:
    """A `name`-prefixed filename within `export_entry_dir(name)`, safe to share a directory with
    other datasets/lookups from the same repo."""
    return export_filename(f"{name}_{stem}")


def entry_commit_file(name: str, commit: str) -> Path:
    """The per-commit source export for a dataset or lookup, shared by every release that resolves to it."""
    return export_entry_dir(name) / entry_export_filename(name, commit)


def entry_export_files(export_dir: Path, name: str) -> list[Path]:
    """Every one of `name`'s own per-commit exports in `export_dir`, keyed `<name>_<commit>`
    where `<commit>` contains no dot, which tells a real export apart from `name`'s own schema
    sidecars/release map sharing the same `<name>_*` prefix and EXPORT_SUFFIX
    (`schema_sidecar_file`'s `.schema.json` tail still ends in `.json`, so under
    `KART_EXPORT_FORMAT=geojson` a loose glob alone can't tell them apart).
    """
    prefix = f"{name}_"
    matches = []
    for candidate in export_dir.glob(f"{prefix}*{EXPORT_SUFFIX}"):
        payload = candidate.name.removeprefix(prefix).removesuffix(EXPORT_SUFFIX)
        if "." not in payload:
            matches.append(candidate)
    return matches


def entry_exported_sentinel(name: str) -> Path:
    """Marks `name`'s export as complete -- the Snakemake-tracked output of `export`.

    `export_entry_dir(name)` itself can't be the tracked output: it's shared with every other
    dataset/lookup sourced from the same repo, so two Snakemake jobs could never both claim it
    as their own output. Kept outside any repo directory because Snakemake rule outputs must be
    a plain wildcard-substituted string, not a function of the wildcard.
    """
    return EXPORT_DIR / ".sentinel" / f"{name}.exported"


def schema_sidecar_file(export_file: Path) -> Path:
    """Where a per-commit export's kart column schema is captured (see `assets.extract.export`),
    read back by `fid_lifecycle`/`transform`/`prepare_lookup` in place of a live `git show`
    against the clone -- so Stage 2 has no dependency on the source clone being present.
    """
    return export_file.parent / f"{export_file.name}.schema.json"


def load_schema_sidecar(export_file: Path) -> tuple[dict, ...]:
    """The schema captured for `export_file` by `assets.extract.export` (see `schema_sidecar_file`)."""
    return tuple(json.loads(schema_sidecar_file(export_file).read_text()))


def releases_map_file(export_dir: Path, name: str) -> Path:
    """Where `name`'s `release id -> {commit, commit_time, location}` map is captured (see
    `write_releases_map`). Named per entry, not just per directory: `export_dir` may be shared by
    several datasets/lookups sourced from the same repo."""
    return export_dir / f"{name}.releases.json"


def write_releases_map(
    export_dir: Path,
    name: str,
    entries: dict[int, dict[str, Any]],
    *,
    dataset_id: str | None = None,
) -> None:
    """Persist each release's resolved commit, commit timestamp and export filename, plus
    `release_config_hash()` (`config`), `source_commit_hash()` (`commit`), and optionally the
    source `dataset_id` so Stage 2 (`fid_lifecycle`, `transform`, `joins`, `fixups`) needs no
    source clone.

    `location` is just a filename (e.g. `ds_<sha>.parquet`), not a path: every commit file a
    dataset's/lookup's release map can point at lives in this exact same `export_dir`.
    """
    payload: dict[str, Any] = {
        "config": release_config_hash(),
        "commit": source_commit_hash(),
        "exported_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "releases": {str(k): v for k, v in entries.items()},
    }
    if dataset_id is not None:
        payload["dataset_id"] = dataset_id
    releases_map_file(export_dir, name).write_text(json.dumps(payload))


def load_releases_metadata(export_dir: Path, name: str) -> dict[str, Any]:
    """The full parsed `<name>.releases.json` document (metadata + releases)."""
    return json.loads(releases_map_file(export_dir, name).read_text())


def load_releases_map(export_dir: Path, name: str) -> dict[int, dict[str, Any]]:
    data = load_releases_metadata(export_dir, name)
    return {int(k): v for k, v in data["releases"].items()}


def resolve_release_export(name: str, release_id: int) -> Path | None:
    """The per-commit export file `name` resolves to for `release_id`, or None if the release
    predates the source's first commit."""
    entry_dir = export_entry_dir(name)
    releases_map = load_releases_map(entry_dir, name)
    info = releases_map.get(release_id)
    if info is None or info.get("location") is None:
        return None
    return entry_dir / info["location"]


def theme_release_file(theme_name: str, release_id: int) -> Path:
    """A release's merged theme file (see `assets.theme`)."""
    return WORKING_THEME_DIR / f"release_{release_id}" / f"{theme_name}{THEME_SUFFIX}"


def theme_hash_file(theme_name: str, release_id: int) -> Path:
    """Sidecar holding the SHA-256 of a release's merged theme file, as `assets.theme` writes it."""
    return theme_release_file(theme_name, release_id).with_name(f"{theme_name}{THEME_SUFFIX}.sha256")
