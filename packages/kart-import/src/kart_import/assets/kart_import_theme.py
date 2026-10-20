import logging
import shutil
from collections.abc import Mapping
from datetime import timedelta
from pathlib import Path
from typing import Any

import pyogrio

from kart_import.log import log_context

from ..command import run_command
from ..config import OUTPUT_DIR, THEME_SUFFIX, WORKING_THEME_DIR, get_releases
from ..git.schema_ids import assign_stable_column_ids

logger = logging.getLogger("kart_import")

PATH_STRUCTURE_ARGS = [
    "--path-encoding",
    "hex",
    "--path-branches",
    "256",
    "--path-levels",
    "2",
]


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


def _write_schema_stub(theme_name: str, sample_file: Path, info: Mapping[str, Any], stub_file: Path) -> None:
    """Writes a zero-row file with `sample_file`'s columns and geometry type, so importing it
    establishes the theme's Kart schema without needing a real release's data.

    `geometry_type` has to be passed through explicitly: an empty GeoDataFrame can't infer one from
    its (absent) geometries, and the resulting "Unknown" type wouldn't match what every real
    release's import declares -- which would mint a second, different schema instead of reusing
    this one.
    """
    stub = pyogrio.read_dataframe(sample_file, where="1=0")
    pyogrio.write_dataframe(
        stub,
        stub_file,
        layer=theme_name,
        driver="FlatGeobuf",
        geometry_type=info["geometry_type"],
        layer_options={"SPATIAL_INDEX": "NO"},
    )


def kart_import_theme(theme_name: str):
    repo_dir = OUTPUT_DIR / "theme" / theme_name
    bundle_file = OUTPUT_DIR / f"{theme_name}.bundle"

    releases = get_releases()

    # Init the Kart repo if it doesn't exist
    if (repo_dir / ".kart").exists():
        logger.info("Removing existing Kart Repo", extra={"target": str(repo_dir)})
        shutil.rmtree(repo_dir)

    repo_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Initializing Kart repo", extra={"target": str(repo_dir)})
    # -b master so the bundle's branch matches what kart_import_repo fetches
    run_command(["kart", "init", "-b", "master", "."], cwd=str(repo_dir))

    pending = []
    for release in releases:
        input_file = WORKING_THEME_DIR / f"release_{release.id}" / f"{theme_name}{THEME_SUFFIX}"
        if not input_file.exists():
            logger.warning(f"Theme file not found: {input_file}. Skipping import.")
            continue

        # `merge_theme_release` symlinks a release's theme file to the previous release's when
        # the two hash identical (see `_dedupe_unchanged_release`), so the symlink itself is the
        # signal: this release is guaranteed to already be imported, no need to read or diff it.
        if input_file.is_symlink():
            logger.info(
                "Theme unchanged from the previous imported release. Skipping import.",
                extra={"release": release.id},
            )
            continue

        # `force_feature_count` because FlatGeobuf reports -1, not 0, for an empty layer.
        info = pyogrio.read_info(input_file, force_feature_count=True)
        if info["features"] == 0:
            logger.info("Theme has no features for this release. Skipping import.", extra={"release": release.id})
            continue

        pending.append((release, input_file, info))

    if pending:
        first_release, stub_sample_file, stub_info = pending[0]
        stub_file = stub_sample_file.with_name(f"_schema_stub{THEME_SUFFIX}")
        _write_schema_stub(theme_name, stub_sample_file, stub_info, stub_file)

        # Must sort chronologically before the first real release commit: kart_import_repo
        # merges themes by commit date, and an unpinned (wall-clock "now") date here would
        # otherwise land this commit wherever "now" falls among the release dates.
        schema_date = (first_release.until - timedelta(seconds=1)).isoformat()
        schema_env = {"GIT_AUTHOR_DATE": schema_date, "GIT_COMMITTER_DATE": schema_date}

        run_command(
            [
                "kart",
                "import",
                "--message",
                f"schema for {theme_name}",
                "--primary-key",
                "id",
                "--replace-existing",
                "--no-checkout",
                *PATH_STRUCTURE_ARGS,
                f"OGR:{geometry_named_vrt(stub_file, theme_name, theme_name)}",
            ],
            cwd=str(repo_dir),
            env=schema_env,
        )
        stub_commit = run_command(["git", "rev-parse", "master"], cwd=str(repo_dir)).strip()

        stable_commit = assign_stable_column_ids(repo_dir, stub_commit, theme_name, env=schema_env)
        run_command(["git", "update-ref", "refs/heads/master", stable_commit], cwd=str(repo_dir))

    for release, input_file, info in pending:
        with log_context(action="kart_import", theme=theme_name, release=release.id):
            # Set commit date using standard Git environment variables
            env = {
                "GIT_AUTHOR_DATE": release.until.isoformat(),
                "GIT_COMMITTER_DATE": release.until.isoformat(),
            }

            import_source = geometry_named_vrt(input_file, theme_name, info["layer_name"])

            logger.info("Importing release", extra={"release": release.id})

            cmd = [
                "kart",
                "import",
                "--message",
                f"feat({theme_name}): import release {release.id}",
                "--primary-key",
                "id",
                "--replace-existing",
                "--no-checkout",
                *PATH_STRUCTURE_ARGS,
                f"OGR:{import_source}",
            ]
            run_command(cmd, cwd=str(repo_dir), env=env, allow_error="No changes to commit")

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
