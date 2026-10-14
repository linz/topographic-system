"""Where a per-commit source export lives, for a dataset or a lookup (see `assets.export`).

Pure path construction, no I/O: importing this costs nothing beyond `config` (no geopandas, no
kart/git subprocess wrappers), so anything that just needs to know a filename - the Snakefile,
tests, a future inspection script - can depend on it without pulling in the export machinery.
"""

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


def lookup_dir(lookup_name: str) -> Path:
    return WORKING_EXPORTS_DIR / "lookup" / lookup_name


def lookup_commit_file(lookup_name: str, commit: str) -> Path:
    """The per-commit source export for a lookup, shared by every release that resolves to it."""
    return lookup_dir(lookup_name) / export_filename(commit)
