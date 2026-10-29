from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import geopandas as gpd

    from kart_import.config import ThemeDataset

MACRON_AS_DIAERESIS: dict[int, int] = {
    0x00E4: 0x0101,  # ä -> ā
    0x00C4: 0x0100,  # Ä -> Ā
    0x00EB: 0x0113,  # ë -> ē
    0x00CB: 0x0112,  # Ë -> Ē
    0x00EF: 0x012B,  # ï -> ī
    0x00CF: 0x012A,  # Ï -> Ī
    0x00F6: 0x014D,  # ö -> ō
    0x00D6: 0x014C,  # Ö -> Ō
    0x00FC: 0x016B,  # ü -> ū
    0x00DC: 0x016A,  # Ü -> Ū
}
"""Source text encodes a macron vowel as the diaeresis form of the same base letter (e.g. ä for
ā), since the legacy system had no macron glyph. `text_encoding` undoes it."""


def text_encoding(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Fix the macron-as-diaeresis encoding bug (see `MACRON_AS_DIAERESIS`) in every free-text
    column of the dataset, e.g. "Whakatäne" -> "Whakatāne". The bug isn't specific to one
    dataset, so this runs over every string column rather than a named list of them."""
    import pandas as pd

    gdf = gdf.copy()
    for column in gdf.columns:
        if column == gdf.geometry.name:
            continue
        if pd.api.types.is_string_dtype(gdf[column].dtype):
            gdf[column] = gdf[column].astype("string").str.translate(MACRON_AS_DIAERESIS)
    return gdf
