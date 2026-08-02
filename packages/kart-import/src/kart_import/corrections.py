"""Declarative value corrections applied to a dataset after field normalization.

A correction either remaps values within one column (`replace`) or sets a column on the
rows where every `where` condition matches (`set`). Matching is type-strict: config keys
match the column's raw values and a type-category mismatch raises (see `kart_import.match`)
rather than silently coercing. They live in the theme config (see ``Correction`` in
``kart_import.config``) and run in ``transform`` after ``normalize_fields``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from .match import require_compatible

if TYPE_CHECKING:
    import geopandas as gpd

    from .config import ThemeDataset


def apply_corrections(gdf: gpd.GeoDataFrame, td: ThemeDataset) -> gpd.GeoDataFrame:
    """Apply the dataset's declarative value corrections, in config order.

    A referenced column that is absent, or a config value whose type does not match its
    column, is a config error and raises.
    """
    for correction in td.corrections:
        needed = [correction.column, *(correction.where or {})]
        missing = [col for col in needed if col not in gdf.columns]
        if missing:
            raise ValueError(f"correction column(s) not found: {missing} in {td.name}")

        column = gdf[correction.column]
        if correction.replace is not None:
            # Match against a snapshot of the original so all pairs apply simultaneously
            snapshot = column.copy()
            for old, new in correction.replace.items():
                require_compatible(snapshot, old, column_name=correction.column, dataset=td.name)
                match = snapshot.eq(old).fillna(False)  # .fillna(False) so NAs never match
                column = column.where(~match, new)
        else:
            mask = pd.Series(True, index=gdf.index)
            for cond_col, cond_val in (correction.where or {}).items():
                condition = gdf[cond_col]
                require_compatible(condition, cond_val, column_name=cond_col, dataset=td.name)
                mask &= condition.eq(cond_val).fillna(False)
            column = column.where(~mask, correction.set_value)
        gdf[correction.column] = column
    return gdf
