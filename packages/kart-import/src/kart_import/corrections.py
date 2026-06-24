"""Declarative value corrections applied to a dataset after field normalization.

A correction either remaps values within one column (`replace`) or sets a column on the
rows where every `where` condition matches (`set`). They live in the theme config (see
``Correction`` in ``kart_import.config``) and run in ``transform`` after ``normalize_fields``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from .match import normalise

if TYPE_CHECKING:
    import geopandas as gpd

    from .config import ThemeDataset


def apply_corrections(gdf: gpd.GeoDataFrame, td: ThemeDataset) -> gpd.GeoDataFrame:
    """Apply the dataset's declarative value corrections, in config order.

    A referenced column that is absent is a config error and raises.
    """
    for correction in td.corrections:
        needed = [correction.column, *(correction.where or {})]
        missing = [col for col in needed if col not in gdf.columns]
        if missing:
            raise ValueError(f"correction column(s) not found: {missing} in {td.name}")

        column = gdf[correction.column]
        if correction.replace is not None:
            # Match against a snapshot of the original so all pairs apply simultaneously
            # (no chaining), and use `where` so a string value upcasts an int column.
            canon = normalise(column)
            for old, new in correction.replace.items():
                column = column.where(canon != normalise(old), new)
        else:
            mask = pd.Series(True, index=gdf.index)
            for cond_col, cond_val in (correction.where or {}).items():
                mask &= normalise(gdf[cond_col]) == normalise(cond_val)
            column = column.where(~mask, correction.set_value)
        gdf[correction.column] = column
    return gdf
