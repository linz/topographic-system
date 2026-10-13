"""Imports every release of one theme into its own throwaway Kart repo, in parallel.

`kart import --replace-existing` mints a fresh random UUID for every column on each run (Kart's
OGR import path, `ColumnSchema.new_id()`), so two independent imports of identical data never
produce matching trees. Parallel workers therefore can't each start from scratch -- every one has
to fork from a single already-established schema so they mint the *same* column UUIDs.

That schema doesn't need to come from a real release: a zero-row stub with the same columns and
geometry type (see `_write_schema_stub`) establishes it just as well, in effectively no time, and
lets every release -- including the first -- run as an equal, parallel worker instead of one
release having to go first and alone.

Each worker is its own disposable `kart init`, with its object writes redirected into the theme
repo's own object store via `GIT_OBJECT_DIRECTORY` and its `master` forced to the schema-stub
commit before importing. Nothing is copied: reads fall through to the shared store, and writes
land there directly, so forking costs nothing but a few KB of repo scaffolding per release.

The results aren't chained into one branch here. Each kept release becomes its own
`refs/heads/release/<id>` ref, sharing the stub as a common (otherwise irrelevant) ancestor; the
actual chronological stitching into one linear per-repo history happens once, for every theme
sharing a target repo, in `kart_import_repo.linearise` -- exactly as it already does for
cross-theme merging, since it only ever reads a commit's own tree, author/committer and message,
never requiring the commits it's given to share real git ancestry.
"""

import logging
import shutil
from pathlib import Path
from typing import NamedTuple

import pyogrio

from kart_import.log import log_context

from ..command import run_command
from ..config import OUTPUT_DIR, THEME_SUFFIX, WORKING_DIR, WORKING_THEME_DIR, Release, get_releases
from ..thread import run_in_thread_pool

logger = logging.getLogger("kart_import")

WORKER_DIR = WORKING_DIR / "kart_import_workers"


def geometry_named_vrt(theme_file: Path, dataset_name: str, source_layer: str) -> Path:
    """Write an OGR VRT beside `theme_file` that names its geometry column `geometry`."""
    vrt_file = theme_file.with_suffix(".vrt")
    vrt_file.write_text(
        "<OGRVRTDataSource>\n"
        f'  <OGRVRTLayer name="{dataset_name}">\n'
        f'    <SrcDataSource relativeToVRT="1">{theme_file.name}</SrcDataSource>\n'
        f"    <SrcLayer>{source_layer}</SrcLayer>\n"
        '    <GeometryField name="geometry" />\n'
        "  </OGRVRTLayer>\n"
        "</OGRVRTDataSource>\n"
    )
    return vrt_file


class _PendingRelease(NamedTuple):
    """A release whose theme file has real data to import this run."""

    release: Release
    input_file: Path
    layer_name: str


def _pending_releases(theme_name: str, releases: list[Release]) -> list[_PendingRelease]:
    pending = []
    for release in releases:
        input_file = WORKING_THEME_DIR / f"release_{release.id}" / f"{theme_name}{THEME_SUFFIX}"
        if not input_file.exists():
            logger.warning(f"Theme file not found: {input_file}. Skipping import.")
            continue

        # `force_feature_count` because FlatGeobuf reports -1, not 0, for an empty layer.
        info = pyogrio.read_info(input_file, force_feature_count=True)
        if info["features"] == 0:
            logger.info("Theme has no features for this release. Skipping import.", extra={"release": release.id})
            continue

        pending.append(_PendingRelease(release, input_file, info["layer_name"]))
    return pending


def _write_schema_stub(theme_name: str, sample_file: Path, stub_file: Path) -> None:
    """Writes a zero-row file with `sample_file`'s columns and geometry type, so importing it
    establishes the theme's Kart schema without needing a real release's data.

    `geometry_type` has to be passed through explicitly: an empty GeoDataFrame can't infer one from
    its (absent) geometries, and the resulting "Unknown" type wouldn't match what every real
    release's import declares -- which would mint a second, different schema instead of reusing
    this one.
    """
    info = pyogrio.read_info(sample_file, force_feature_count=True)
    stub = pyogrio.read_dataframe(sample_file, where="1=0")
    pyogrio.write_dataframe(stub, stub_file, layer=theme_name, driver="FlatGeobuf", geometry_type=info["geometry_type"])


def _kart_import_cmd(theme_name: str, release_id: int, import_source: Path) -> list[str]:
    return [
        "kart",
        "import",
        "--message",
        f"import {theme_name} for release {release_id}",
        "--primary-key",
        "id",
        "--replace-existing",
        "--no-checkout",
        f"OGR:{import_source}",
    ]


def _import_release_worker(repo_dir: Path, seed_commit: str, theme_name: str, item: _PendingRelease) -> str:
    """Imports one release into a throwaway, otherwise-empty Kart repo whose object writes land
    directly in `repo_dir`'s own object store, forked from `seed_commit` so it reuses the schema
    (and column UUIDs) that commit already established. Returns the resulting commit sha.
    """
    worker_dir = WORKER_DIR / theme_name / f"release_{item.release.id}"
    shutil.rmtree(worker_dir, ignore_errors=True)
    worker_dir.mkdir(parents=True)
    try:
        run_command(["kart", "init", "-b", "master", "."], cwd=str(worker_dir))

        env = {
            "GIT_OBJECT_DIRECTORY": str(repo_dir / ".kart" / "objects"),
            "GIT_AUTHOR_DATE": item.release.until.isoformat(),
            "GIT_COMMITTER_DATE": item.release.until.isoformat(),
        }
        run_command(["git", "update-ref", "refs/heads/master", seed_commit], cwd=str(worker_dir), env=env)

        import_source = geometry_named_vrt(item.input_file, theme_name, item.layer_name)
        run_command(_kart_import_cmd(theme_name, item.release.id, import_source), cwd=str(worker_dir), env=env)

        return run_command(["git", "rev-parse", "master"], cwd=str(worker_dir)).strip()
    finally:
        shutil.rmtree(worker_dir, ignore_errors=True)


def _dedupe_unchanged_releases(
    repo_dir: Path, results: list[tuple[_PendingRelease, str]]
) -> list[tuple[_PendingRelease, str]]:
    """Drops a release whose resulting tree is identical to the previous *kept* release's --
    every worker forks from the same schema-only stub, so a sequential `kart import
    --replace-existing` would no longer report "No changes to commit" for a release that merely
    repeats its immediate predecessor; this restores that collapsing as a cheap, after-the-fact
    tree-sha comparison instead.
    """
    kept = []
    previous_tree = None
    for item, commit in results:
        tree = run_command(["git", "rev-parse", f"{commit}^{{tree}}"], cwd=str(repo_dir)).strip()
        if tree == previous_tree:
            logger.info("No changes to commit", extra={"release": item.release.id})
            continue
        kept.append((item, commit))
        previous_tree = tree
    return kept


def kart_import_theme(theme_name: str, thread_count: int = 4) -> str:
    repo_dir = OUTPUT_DIR / "theme" / theme_name
    bundle_file = OUTPUT_DIR / f"{theme_name}.bundle"
    worker_root = WORKER_DIR / theme_name

    if (repo_dir / ".kart").exists():
        logger.info("Removing existing Kart Repo", extra={"target": str(repo_dir)})
        shutil.rmtree(repo_dir)
    shutil.rmtree(worker_root, ignore_errors=True)

    repo_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Initializing Kart repo", extra={"target": str(repo_dir)})
    run_command(["kart", "init", "-b", "master", "."], cwd=str(repo_dir))

    pending = _pending_releases(theme_name, get_releases())
    if pending:
        worker_root.mkdir(parents=True, exist_ok=True)
        stub_file = worker_root / f"_schema_stub{THEME_SUFFIX}"
        _write_schema_stub(theme_name, pending[0].input_file, stub_file)
        run_command(
            ["kart", "import", "--message", "schema stub", "--primary-key", "id", "--replace-existing", "--no-checkout", f"OGR:{stub_file}"],
            cwd=str(repo_dir),
        )
        seed_commit = run_command(["git", "rev-parse", "master"], cwd=str(repo_dir)).strip()
        shutil.rmtree(worker_root, ignore_errors=True)

        logger.info(f"Importing {len(pending)} release(s) in parallel", extra={"thread_count": thread_count})

        def worker(item: _PendingRelease) -> tuple[_PendingRelease, str]:
            with log_context(action="import_release", theme=theme_name, release=item.release.id):
                return item, _import_release_worker(repo_dir, seed_commit, theme_name, item)

        results = run_in_thread_pool(func=worker, items=pending, thread_count=thread_count, trace_items=False)

        for item, commit in _dedupe_unchanged_releases(repo_dir, results):
            run_command(["git", "update-ref", f"refs/heads/release/{item.release.id}", commit], cwd=str(repo_dir))
            logger.info("Imported release", extra={"release": item.release.id})

    run_command(["kart", "git", "bundle", "create", str(bundle_file), "--all"], cwd=str(repo_dir))
    logger.info("All releases imported")
    return str(repo_dir)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m kart_import.assets.kart_import_theme <theme_name>")
        sys.exit(1)
    with log_context(action="kart_import_theme", theme=sys.argv[1]):
        kart_import_theme(sys.argv[1])
