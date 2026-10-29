import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from .config import ThemeDataset
from .fixups_text_encoding import text_encoding


def _td(fixups_cfg: list[dict], name: str = "t") -> ThemeDataset:
    return ThemeDataset.model_validate(
        {"name": name, "source": "kart@data.koordinates.com:linz/x-topo-150k", "fixups": fixups_cfg}
    )


def test_text_encoding_fixes_macron_as_diaeresis_in_the_name_columns():
    """Diaeresis characters standing in for macron vowels are fixed in `full_text`/`text_string`
    - not any dataset carrying a free-text column under some other name - while a non-text
    string column and a null value pass through unchanged."""
    gdf = gpd.GeoDataFrame(
        {
            "full_text": pd.array(["Whakatäne", "Taupo", None], dtype="string"),
            "text_string": pd.array(["Pöhangina", "Taupo", None], dtype="string"),
            "style": pd.array(["Narrow", "Narrow", "Narrow"], dtype="string"),
        },
        geometry=[Point(0, 0), Point(1, 1), Point(2, 2)],
        crs="EPSG:2193",
    )
    out = text_encoding(gdf, _td([], "linz_carto_text"), 66)

    assert out["full_text"].tolist() == ["Whakatāne", "Taupo", pd.NA]
    assert out["text_string"].tolist() == ["Pōhangina", "Taupo", pd.NA]
    assert out["style"].tolist() == ["Narrow", "Narrow", "Narrow"]


def test_text_encoding_skips_a_dataset_without_the_name_columns():
    """A dataset that doesn't carry `full_text`/`text_string` (e.g. a lookup joined column
    happens to be named `name`) is left untouched rather than guessed at."""
    gdf = gpd.GeoDataFrame(
        {"name": pd.array(["Pöhangina"], dtype="string")},
        geometry=[Point(0, 0)],
        crs="EPSG:2193",
    )
    out = text_encoding(gdf, _td([], "river_cl_lkp"), 66)

    assert out["name"].tolist() == ["Pöhangina"]
