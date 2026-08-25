"""Dataset fixups: per-dataset, release-aware patches.

A fixup is an arbitrary function that takes a (already field-normalized)
GeoDataFrame plus the release id and returns the corrected frame. Use them for
one-off data repairs that can't be expressed declaratively in a theme's
`mapping` or `corrections` - setting a column on specific records, or dropping
records the source publishes broken (see `drop_degenerate_fences`).

Register a fixup by adding it to ``FIXUPS``; reference it from a dataset in the
theme config:

    fixups:
      - fn: drop_empty_residential_areas
        releases: [51]   # omit `releases` to apply to every release

Config loading validates that every referenced ``fn`` exists here, so a typo
fails at load time rather than mid-run. A fixup gated to a release that shares
its transform with an earlier one is rejected too, since it would never run;
gate it to that earlier release instead.

Prefer identifying records by a stable key (t50_fid) over anything positional,
and where a repair can be skipped safely - a record already fixed upstream -
gate on the data rather than the release, so a stale fixup fades out on its own
rather than corrupting a later snapshot.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

logger = logging.getLogger("kart_import")


if TYPE_CHECKING:
    import geopandas as gpd

    from kart_import.config import ThemeDataset

# (gdf, td, release_id) -> gdf
Fixup = Callable[["gpd.GeoDataFrame", "ThemeDataset", int], "gpd.GeoDataFrame"]


def _drop_listed_empty(
    gdf: gpd.GeoDataFrame,
    td: ThemeDataset,
    dataset: str,
    t50_fids: set[int],
) -> gpd.GeoDataFrame:
    """Drop the listed t50_fids of `dataset`, but only where the geometry is actually missing.

    The FlatGeobuf driver refuses an empty or NULL geometry while building a spatial index
    ("NULL geometry not supported with spatial index"), so one such feature fails the whole
    theme write.

    Gating on the geometry rather than the release is what lets the fid list be permanent: a
    fid whose geometry is present is kept and logged, so a feature repaired upstream is not
    deleted forever by a list nobody thought to trim.

    `dataset` names what the fid list was checked against. Wired to any other dataset the fids
    would simply match nothing, so the mistake would pass as a successful no-op build rather
    than an error.
    """
    import pandas as pd

    if td.name != dataset:
        raise ValueError(f"fixup for dataset '{dataset}' applied to '{td.name}'")

    fids = pd.to_numeric(gdf["t50_fid"], errors="coerce")
    listed = fids.isin(t50_fids)
    if not listed.any():
        return gdf

    # `isna` as well as `is_empty`: a NULL geometry fails the FlatGeobuf write the same way.
    missing = gdf.geometry.is_empty | gdf.geometry.isna()

    # Warn rather than drop: the fid is on the list but the source now carries a geometry for it,
    # so the list has outlived the defect and this fid can come off it.
    if (repaired := listed & ~missing).any():
        logger.warning(
            "listed fid now has geometry, keeping it",
            extra={"t50_fids": sorted(fids[repaired].astype(int).tolist())},
        )

    drop = listed & missing
    if not drop.any():
        return gdf

    logger.info(
        "dropping listed fids with no geometry",
        extra={"t50_fids": sorted(fids[drop].astype(int).tolist())},
    )
    return gdf[~drop].reset_index(drop=True)


def drop_degenerate_fences(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Drop four zero-length nz_fence_centrelines features.

    All four are the same 0.5 micrometre two-vertex line in EPSG:2193
    (1756000.000000001 5420267.181827734 -> ...182310526), present from the 2020-02-16 source
    snapshot on. Both vertices land in the same 1e-8 degree cell, so `set_precision` collapses
    them to LINESTRING EMPTY.

    Absent in releases 52-55 and back in 56+, hence the geometry gate rather than a release gate;
    see `_drop_listed_empty`.
    """
    return _drop_listed_empty(gdf, td, "nz_fence_centrelines", {7640059, 7640098, 7704786, 7704787})


def drop_empty_residential_areas(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Drop the one nz_residential_area_polygons feature the source publishes with no geometry.

    t50_fid 6753838 (Taihape): the 2020-11-17 snapshot (release 51) is the only one where
    the source contains a null geometry for this fid, and `kart export` reproduces that null verbatim.

    Gated to release 51 in the config. The fid is present in all 21 snapshots, so an ungated fixup
    would log the "now has geometry" warning on 20 of them.
    """
    return _drop_listed_empty(gdf, td, "nz_residential_area_polygons", {6753838})


def _split_id(parent_id: str, dataset_name: str, parent_fid, part_index: int) -> str:
    """Deterministic UUIDv7 for a part that has no source fid.

    Reuses the parent's 48-bit timestamp prefix (so the derived record sorts with the feature it
    came from) and hashes a key that cannot collide with a key from the lifecycle.
    """
    import uuid

    from .uuid7 import reproducable_uuid7_text

    try:
        timestamp_ms = int(uuid.UUID(str(parent_id)).hex[:12], 16)
    except ValueError:
        timestamp_ms = 0
    return str(reproducable_uuid7_text(timestamp_ms, f"{dataset_name}:{parent_fid}:part{part_index}"))


def split_multipart_features(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Explode multipart geometries into one record per part.

    The largest part stays on the original row (keeping its id, `t50_fid` and attributes); the
    remaining parts are appended as new records carrying the same attributes but a null `t50_fid`
    and a derived id. Single-part features are untouched, so a layer with no splits comes back
    unchanged.
    """
    import geopandas as gpd_
    import pandas as pd

    multipart = gdf.geometry.geom_type.isin(("MultiPolygon", "MultiLineString", "MultiPoint"))
    if not multipart.any():
        return gdf

    gdf = gdf.reset_index(drop=True)
    extra_rows = []

    for position in gdf.index[multipart.to_numpy()]:
        row = gdf.loc[position]
        parts = sorted(row.geometry.geoms, key=lambda part: part.area or part.length, reverse=True)
        parent_fid = row["t50_fid"] if "t50_fid" in gdf.columns else None

        # The largest part keeps the source feature's identity.
        gdf.at[position, "geometry"] = parts[0]

        for part_index, part in enumerate(parts[1:], start=1):
            new_row = row.copy()
            new_row["geometry"] = part
            new_row["id"] = _split_id(row["id"], td.name, parent_fid, part_index)
            if "t50_fid" in gdf.columns:
                new_row["t50_fid"] = pd.NA
            extra_rows.append(new_row)

        logger.info(
            "split multipart feature",
            extra={
                "dataset": td.name,
                "release": release_id,
                "t50_fid": None if parent_fid is None or pd.isna(parent_fid) else int(parent_fid),
                "parts": len(parts),
                "kept": float(parts[0].area or parts[0].length),
                "split_off": [float(p.area or p.length) for p in parts[1:]],
            },
        )

    if not extra_rows:
        return gdf

    added = gpd_.GeoDataFrame(extra_rows, columns=gdf.columns, crs=gdf.crs).set_geometry("geometry")
    out = pd.concat([gdf, added], ignore_index=True)
    return gpd_.GeoDataFrame(out, geometry="geometry", crs=gdf.crs)


def drop_degenerate_tracks(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Drop the zero-length nz_track_centrelines features."""
    return _drop_listed_empty(
        gdf,
        td,
        "nz_track_centrelines",
        {
            7708693,
            8118480,
            8337121,
            8337123,
            8337124,
            8337127,
            8337128,
            8337131,
            8337324,
            8337326,
            8337327,
            8337329,
            8337330,
            8337331,
            8494016,
            8494018,
            8511453,
            8511454,
            8511455,
            8511457,
            8511468,
            8511469,
            8532815,
            8532817,
            8532818,
            8532821,
            8532822,
            8532824,
            8532825,
        },
    )


FIXUPS: dict[str, Fixup] = {
    "drop_degenerate_fences": drop_degenerate_fences,
    "drop_degenerate_tracks": drop_degenerate_tracks,
    "drop_empty_residential_areas": drop_empty_residential_areas,
    "split_multipart_features": split_multipart_features,
}
