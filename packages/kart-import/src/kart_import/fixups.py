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


def _match_fids(gdf: gpd.GeoDataFrame, fids: set[int]):
    """Boolean mask of rows whose `fid` is in `fids`, robust to int/float dtypes
    (pyogrio may read an integer fid as float)."""
    import pandas as pd

    return pd.to_numeric(gdf["fid"], errors="coerce").isin(fids)


def change_type_to_none(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Demo function that sets change type to none for specific fids"""
    logger.info(f"Fixing {release_id=} for {td.name}")
    mask = _match_fids(gdf, {3198908, 3198849})
    gdf.loc[mask, "change_type"] = None
    return gdf


FIXUPS: dict[str, Fixup] = {
    "change_type_to_none": change_type_to_none,
}
