import hashlib
import json
import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

from ..config import (
    WORKING_LIFECYCLE_DIR,
    Release,
    ThemeDataset,
    get_dataset_by_name,
    get_releases,
)
from ..export_paths import (
    export_entry_dir,
    load_releases_map,
    load_releases_metadata,
    load_schema_sidecar,
    schema_sidecar_file,
)
from ..kart_types import coerce_integer_columns
from ..log import log_context
from ..uuid7 import reproducable_uuid7_text
from .extract.export import gpd_read

logger = logging.getLogger("kart_import")


def get_fid_lifecycle_file(dataset_name: str, releases: list[Release]) -> Path:
    """
    FID output lifecycle file location

    This varies based on which releases are expected
    """
    return WORKING_LIFECYCLE_DIR / f"{dataset_name}_release{releases[0].id}-{releases[-1].id}.json"


def make_lifecycle_id(commit_time: str, fid: str, fid_field: str, dataset_id: str) -> str:
    """Generate a reproducible UUID for a lifecycle entry."""
    ts_ms = int(datetime.fromisoformat(commit_time).timestamp() * 1000)
    if fid_field == "auto_pk":
        return str(reproducable_uuid7_text(ts_ms, f"{dataset_id}:{fid}"))
    return str(reproducable_uuid7_text(ts_ms, fid))


def detect_fid_field(td: ThemeDataset, export_files: list[Path]) -> str:
    """Determine which field identifies features across the dataset's history.

    Uses `td.feature_key` if configured, otherwise `t50_fid` if any exported commit's schema
    carries it, falling back to `auto_pk`.
    """
    if td.feature_key:
        logger.info(f"Using configured feature_key {td.feature_key!r} for {td.name}")
        return td.feature_key

    for export_file in export_files:
        sidecar = schema_sidecar_file(export_file)
        if sidecar.exists():
            col_names = {col.get("name") for col in load_schema_sidecar(export_file)}
        else:
            col_names = set(gpd_read(export_file).columns)
        if "t50_fid" in col_names:
            logger.info(f"Using t50_fid for {td.name}")
            return "t50_fid"

    logger.info(f"No t50_fid column found for {td.name}, using auto_pk")
    return "auto_pk"


def _resolve_pk_column(gdf: gpd.GeoDataFrame, fid_field: str) -> str:
    if fid_field in gdf.columns:
        return fid_field
    if fid_field == "auto_pk" and "fid" in gdf.columns:
        # topographic-source-data renamed auto_pk to fid in its release 67 reload
        return "fid"
    raise KeyError(f"Primary key column {fid_field!r} not found in export columns: {list(gdf.columns)}")


def _frame_row_digests(gdf: gpd.GeoDataFrame, pk_col: str) -> tuple[dict[str, bytes], list[str]]:
    """Compute a deterministic digest per feature row so consecutive commits can be diffed."""
    geom_col = gdf.geometry.name
    attr_cols = sorted(col for col in gdf.columns if col != geom_col)
    if attr_cols:
        attr_hashes = pd.util.hash_pandas_object(gdf[attr_cols], index=False).to_numpy()
    else:
        attr_hashes = np.zeros(len(gdf), dtype=np.uint64)
    geom_wkbs = shapely.to_wkb(gdf.geometry.to_numpy())
    fids = gdf[pk_col].astype(str).to_numpy()

    digests: dict[str, bytes] = {}
    duplicates: list[str] = []
    for fid_str, attr_h, wkb in zip(fids, attr_hashes, geom_wkbs, strict=True):
        if fid_str in digests:
            duplicates.append(fid_str)
            continue
        h = hashlib.blake2b(digest_size=16)
        h.update(int(attr_h).to_bytes(8, "little", signed=False))
        if wkb is not None:
            h.update(wkb)
        digests[fid_str] = h.digest()
    return digests, duplicates


def _parse_iso_utc(value: str | datetime) -> datetime:
    dt = datetime.fromisoformat(value) if isinstance(value, str) else value
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def updated_at_for_release(entry: Mapping[str, Any], as_of: datetime | None = None) -> str | None:
    """Return the feature's `updated_at` timestamp as of `as_of` (a release's cutoff),
    so earlier releases never inherit a future modification timestamp."""
    created_at: str | None = entry.get("created_at")
    if as_of is None:
        return entry.get("updated_at") or created_at

    cutoff = _parse_iso_utc(as_of)
    best = created_at
    updates: list[str] | None = entry.get("updates")
    if updates is not None:
        for ts in updates:
            if _parse_iso_utc(ts) <= cutoff:
                best = ts
    else:
        # Legacy lifecycle.json without per-change `updates` list
        updated_at: str | None = entry.get("updated_at")
        if updated_at is not None and _parse_iso_utc(updated_at) <= cutoff:
            best = updated_at
    return best


def update_lifecycle_from_frame(
    gdf: gpd.GeoDataFrame,
    lifecycle: dict[str, dict[str, Any]],
    prev_digests: dict[str, bytes],
    commit_time: str,
    dataset_id: str,
    fid_field: str,
) -> dict[str, bytes]:
    """Update `lifecycle` with features added or modified in `gdf` relative to `prev_digests`.
    Returns this commit's feature digests for the next commit's comparison."""
    if gdf.empty:
        return {}

    pk_col = _resolve_pk_column(gdf, fid_field)
    curr_digests, skipped_fids = _frame_row_digests(gdf, pk_col)

    for fid_str, digest in curr_digests.items():
        entry = lifecycle.get(fid_str)
        if entry is None:
            lifecycle[fid_str] = {
                "id": make_lifecycle_id(commit_time, fid_str, fid_field, dataset_id),
                "created_at": commit_time,
                "updated_at": commit_time,
            }
        elif prev_digests.get(fid_str) != digest:
            entry["updated_at"] = commit_time
            updates: list[str] = entry.setdefault("updates", [])
            if not updates or updates[-1] != commit_time:
                updates.append(commit_time)

    if skipped_fids:
        logger.info(f"skipped {len(skipped_fids)} duplicate fids.")
        for i, fid_str in enumerate(skipped_fids[:10]):
            logger.debug(f"{i: 4}/{len(skipped_fids)}: [{fid_str}] previously seen: {lifecycle[fid_str]['created_at']}")

    return curr_digests


def generate_lifecycle(dataset_name: str) -> str:
    """Derive each feature's stable UUIDv7 `id`, `created_at` and `updated_at` by walking the
    dataset's exported per-commit parquets in release order (no source clone needed)."""
    td = get_dataset_by_name(dataset_name)
    releases = get_releases()
    entry_dir = export_entry_dir(dataset_name)
    meta = load_releases_metadata(entry_dir, dataset_name)
    releases_map = load_releases_map(entry_dir, dataset_name)
    dataset_id: str = meta.get("dataset_id") or td.source.dataset or dataset_name

    # Distinct commits in release order; consecutive releases sharing a commit are diffed once.
    steps: list[tuple[str, str, Path]] = []
    last_commit: str | None = None
    for release in releases:
        info = releases_map.get(release.id)
        if not info or info.get("commit") is None or info.get("location") is None:
            logger.debug(f"No commit for release {release.id}. Skipping.")
            continue
        commit = info["commit"]
        if commit == last_commit:
            logger.debug(f"Release {release.id} has same commit as previous. Skipping diff.")
            continue
        steps.append((commit, info["commit_time"], entry_dir / info["location"]))
        last_commit = commit

    fid_field = detect_fid_field(td, [export_file for _, _, export_file in steps])

    lifecycle: dict[str, dict[str, Any]] = {}
    prev_digests: dict[str, bytes] = {}
    for commit, commit_time, export_file in steps:
        gdf = gpd_read(export_file)
        if not gdf.empty and schema_sidecar_file(export_file).exists():
            gdf = coerce_integer_columns(
                gdf,
                load_schema_sidecar(export_file),
                context=f"lifecycle {dataset_name!r} at {commit}",
            )
        prev_digests = update_lifecycle_from_frame(gdf, lifecycle, prev_digests, commit_time, dataset_id, fid_field)

    WORKING_LIFECYCLE_DIR.mkdir(parents=True, exist_ok=True)
    output_file = get_fid_lifecycle_file(dataset_name, releases)
    with open(output_file, "w") as f:
        json.dump(lifecycle, f, indent=2)
    logger.info("write lifecycle", extra={"fids": len(lifecycle), "target": output_file})

    return str(output_file)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m kart_import.assets.fid_lifecycle <dataset_name>")
        sys.exit(1)

    with log_context(action="lifecycle", dataset=sys.argv[1]):
        generate_lifecycle(sys.argv[1])
