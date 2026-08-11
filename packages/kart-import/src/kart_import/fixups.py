"""Dataset fixups: per-dataset, release-aware patches.

A fixup is an arbitrary function that takes a (already field-normalized)
GeoDataFrame plus the release id and returns the corrected frame. Use them for
one-off data repairs that can't be expressed declaratively in a theme's
`mapping` - e.g. "set these columns on these specific records for these
releases".

Register a fixup by adding it to ``FIXUPS``; reference it from a dataset in the
theme config:

    fixups:
      - fn: repair_broken_railway_data
        releases: [64, 65]   # omit `releases` to apply to every release

Config loading validates that every referenced ``fn`` exists here, so a typo
fails at load time rather than mid-run.
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


def drop_degenerate_fences(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Drop four zero-length nz_fence_centrelines features.

    All four are the same 0.5 micrometre two-vertex line in EPSG:2193
    (1756000.000000001 5420267.181827734 -> ...182310526), present from the 2020-02-16 source
    snapshot on. Both vertices land in the same 1e-8 degree cell, so `set_precision` collapses
    them to LINESTRING EMPTY, and the FlatGeobuf driver refuses an empty geometry while
    building a spatial index ("NULL geometry not supported with spatial index").

    Ungated on release: the fids come and go across snapshots (absent in releases 52-55, back
    in 56+), and a fixup can't be gated to a release that shares its transform with another.
    A release without them is a no-op.

    Gated on the geometry instead: only a listed fid that has actually collapsed is dropped. If
    a fence was repaired upstream it stays in place in the theme rather than being deleted forever
    by a stale fid list, and the retained fid is logged so the list can be trimmed.
    """
    import pandas as pd

    listed = pd.to_numeric(gdf["t50_fid"], errors="coerce").isin({7640059, 7640098, 7704786, 7704787})
    if not listed.any():
        return gdf

    # `isna` as well as `is_empty`: a NULL geometry fails the FlatGeobuf write the same way.
    collapsed = gdf.geometry.is_empty | gdf.geometry.isna()

    if (repaired := listed & ~collapsed).any():
        logger.warning(
            "degenerate fence centreline now has geometry, keeping it",
            extra={
                "dataset": td.name,
                "release": release_id,
                "t50_fids": sorted(gdf.loc[repaired, "t50_fid"].tolist()),
            },
        )

    drop = listed & collapsed
    if not drop.any():
        return gdf

    logger.info(
        "dropping degenerate fence centrelines",
        extra={
            "dataset": td.name,
            "release": release_id,
            "count": int(drop.sum()),
            "t50_fids": sorted(gdf.loc[drop, "t50_fid"].tolist()),
        },
    )
    return gdf[~drop].reset_index(drop=True)


FIXUPS: dict[str, Fixup] = {
    "drop_degenerate_fences": drop_degenerate_fences,
}
