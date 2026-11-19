import functools
import hashlib
import json
from pathlib import Path

from .command import run_command
from .config import (
    CONFIG_DIR_RELEASE,
    EXPORT_DIR,
    EXPORT_SUFFIX,
    WORKING_EXPORTS_DIR,
    get_source_entry,
    get_source_repo_name,
)


def release_config_hash() -> str:
    """A hash of `config/topo50_release.yml`'s current content, embedded in every release map
    (see `write_releases_map`) and in `assets.working_data`'s `summary.json`, so a release
    definition change (a cutoff date edited, a release added) is visible even before it has
    changed any dataset's resolved commits."""
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


def dataset_release_file(dataset_name: str, release_id: int) -> Path:
    """Where a release's export is linked for a dataset (see `assets.export.link_release_export`).
    Purely local/derived -- never synced, and rebuildable from the dataset's release map."""
    return WORKING_EXPORTS_DIR / f"release_{release_id}" / export_filename(dataset_name)


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


def entry_export_files(export_dir: Path, name: str) -> list[Path]:
    """Every one of `name`'s own per-commit exports in `export_dir`, keyed `<name>_<payload>`
    where `payload` is a git commit sha or the literal `empty` -- never containing a dot, which is
    what tells a real export apart from `name`'s own schema sidecars/release map sharing the same
    `<name>_*` prefix and EXPORT_SUFFIX (`schema_sidecar_file`'s `.schema.json` tail still ends in
    `.json`, so under `KART_EXPORT_FORMAT=geojson` a loose glob alone can't tell them apart).
    """
    prefix = f"{name}_"
    matches = []
    for candidate in export_dir.glob(f"{prefix}*{EXPORT_SUFFIX}"):
        payload = candidate.name.removeprefix(prefix).removesuffix(EXPORT_SUFFIX)
        if "." not in payload:
            matches.append(candidate)
    return matches


def entry_exported_sentinel(name: str) -> Path:
    """Marks `name`'s export as complete -- the Snakemake-tracked output of `export_lookup`.

    A lookup has no per-release symlink view (unlike a dataset, whose `dataset_release_file`s
    already serve that role), and `export_entry_dir(name)` itself can't be the tracked output
    either: it's shared with every other dataset/lookup sourced from the same repo, so two
    Snakemake jobs could never both claim it as their own output. Kept outside any repo directory
    entirely (rather than just under a different name within one) because Snakemake rule outputs
    must be a plain wildcard-substituted string, not a function of the wildcard -- and which repo
    a given lookup belongs to is exactly that, a function.
    """
    return EXPORT_DIR / ".sentinel" / f"{name}.exported"


def dataset_commit_file(dataset_name: str, commit: str) -> Path:
    """The per-commit source export for a dataset, shared by every release that resolves to it."""
    return export_entry_dir(dataset_name) / entry_export_filename(dataset_name, commit)


def dataset_empty_file(dataset_name: str) -> Path:
    """The shared placeholder for releases predating a dataset's first commit
    (see `assets.export.export_empty_releases`)."""
    return export_entry_dir(dataset_name) / entry_export_filename(dataset_name, "empty")


def lookup_commit_file(lookup_name: str, commit: str) -> Path:
    """The per-commit source export for a lookup, shared by every release that resolves to it."""
    return export_entry_dir(lookup_name) / entry_export_filename(lookup_name, commit)


def schema_sidecar_file(export_file: Path) -> Path:
    """Where a per-commit export's kart column schema is captured (see `assets.export`), read back
    by `transform`/`prepare_lookup` in place of a live `git show` against the clone -- so those
    stages have no dependency on the source clone being present.

    `export_lookup`/`prepare_lookup` tell this apart from a real export sharing the same
    directory via `entry_export_files`'s commit-shaped payload check, not via the filename itself.
    """
    return export_file.parent / f"{export_file.name}.schema.json"


def load_schema_sidecar(export_file: Path) -> tuple[dict, ...]:
    """The schema captured for `export_file` by `assets.export` (see `schema_sidecar_file`)."""
    return tuple(json.loads(schema_sidecar_file(export_file).read_text()))


def releases_map_file(export_dir: Path, name: str) -> Path:
    """Where `name`'s `release id -> {commit, location}` map is captured (see
    `write_releases_map`). Named per entry, not just per directory: `export_dir` may be shared by
    several datasets/lookups sourced from the same repo."""
    return export_dir / f"{name}.releases.json"


def write_releases_map(export_dir: Path, name: str, entries: dict[int, dict[str, str | None]]) -> None:
    """Persist each release's resolved commit and the export file it resolves to, plus
    `release_config_hash()` (`config`) and `source_commit_hash()` (`commit`) so the file is
    self-describing about which `topo50_release.yml` and which version of the pipeline itself
    built it. Read back by `assets.export.relink_dataset_releases` and
    `joins._resolve_lookup_commit`.

    `location` is just a filename (e.g. `ds_<sha>.parquet`), not a path: every commit file a
    dataset's/lookup's release map can point at lives in this exact same `export_dir`, so there's
    nothing for a path to add.
    """
    payload = {
        "config": release_config_hash(),
        "commit": source_commit_hash(),
        "releases": {str(k): v for k, v in entries.items()},
    }
    releases_map_file(export_dir, name).write_text(json.dumps(payload))


def load_releases_map(export_dir: Path, name: str) -> dict[int, dict[str, str | None]]:
    data = json.loads(releases_map_file(export_dir, name).read_text())
    return {int(k): v for k, v in data["releases"].items()}
