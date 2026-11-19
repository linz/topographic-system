import json
from pathlib import Path

from .config import EXPORT_SUFFIX, WORKING_EXPORTS_DIR


def export_filename(stem: str) -> str:
    """The filename for a per-commit source export (see EXPORT_FORMAT); every reader/writer of
    an export, in Python or the Snakefile, builds the name through this rather than appending
    EXPORT_SUFFIX itself."""
    return f"{stem}{EXPORT_SUFFIX}"


def dataset_release_file(dataset_name: str, release_id: int) -> Path:
    """Where a release's export is linked for a dataset (see `assets.export.link_release_export`)."""
    return WORKING_EXPORTS_DIR / f"release_{release_id}" / export_filename(dataset_name)


def dataset_commit_file(dataset_name: str, commit: str, commit_time: str) -> Path:
    """The per-commit source export for a dataset, shared by every release that resolves to it."""
    return WORKING_EXPORTS_DIR / dataset_name / export_filename(f"{commit_time[:10]}_{commit}")


def dataset_empty_file(dataset_name: str) -> Path:
    """The shared placeholder for releases predating a dataset's first commit
    (see `assets.export.export_empty_releases`)."""
    return WORKING_EXPORTS_DIR / dataset_name / export_filename("empty")


def dataset_export_dir(dataset_name: str) -> Path:
    """Where a dataset's per-commit exports, schema sidecars and release map live."""
    return WORKING_EXPORTS_DIR / dataset_name


def relative_export_path(path: Path) -> str:
    """`path`'s location relative to `WORKING_EXPORTS_DIR`, as recorded in a release map (see
    `write_releases_map`) so it's machine-independent and survives an S3 round-trip."""
    return path.relative_to(WORKING_EXPORTS_DIR).as_posix()


def resolve_export_path(location: str) -> Path:
    """The inverse of `relative_export_path`."""
    return WORKING_EXPORTS_DIR / location


def lookup_dir(lookup_name: str) -> Path:
    return WORKING_EXPORTS_DIR / "lookup" / lookup_name


def lookup_commit_file(lookup_name: str, commit: str) -> Path:
    """The per-commit source export for a lookup, shared by every release that resolves to it."""
    return lookup_dir(lookup_name) / export_filename(commit)


def schema_sidecar_file(export_file: Path) -> Path:
    """Where a per-commit export's kart column schema is captured (see `assets.export`), read back
    by `transform`/`prepare_lookup` in place of a live `git show` against the clone -- so those
    stages have no dependency on the source clone being present.

    Named with a leading dot: `export_lookup`/`prepare_lookup` glob their export dir with
    `export_filename("*")` (`*.parquet`, or `*.json` under `KART_EXPORT_FORMAT=geojson`), and a
    non-dotted sidecar name would collide with that glob and get read back as a geo file. `glob`
    already excludes dotfiles from a bare `*`, so this is safe in either format.
    """
    return export_file.parent / f".{export_file.name}.schema.json"


def load_schema_sidecar(export_file: Path) -> tuple[dict, ...]:
    """The schema captured for `export_file` by `assets.export` (see `schema_sidecar_file`)."""
    return tuple(json.loads(schema_sidecar_file(export_file).read_text()))


def releases_map_file(export_dir: Path) -> Path:
    """Where a dataset's or lookup's `release id -> {commit, location}` map is captured (see
    `write_releases_map`). Same leading-dot safety as `schema_sidecar_file`."""
    return export_dir / ".releases.json"


def write_releases_map(export_dir: Path, entries: dict[int, dict[str, str | None]]) -> None:
    """Persist each release's resolved commit and the export file it resolves to (relative to
    `WORKING_EXPORTS_DIR`, so it survives a move between machines/S3), read back by
    `assets.export.relink_dataset_releases` and `joins._resolve_lookup_commit`."""
    releases_map_file(export_dir).write_text(json.dumps({str(k): v for k, v in entries.items()}))


def load_releases_map(export_dir: Path) -> dict[int, dict[str, str | None]]:
    data = json.loads(releases_map_file(export_dir).read_text())
    return {int(k): v for k, v in data.items()}
