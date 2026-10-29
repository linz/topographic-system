import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from .config import ThemeDataset
from .fixups_text_encoding import text_encoding


def _td(fixups_cfg: list[dict], name: str = "t") -> ThemeDataset:
    return ThemeDataset.model_validate(
        {"name": name, "source": "kart@data.koordinates.com:linz/x-topo-150k", "fixups": fixups_cfg}
    )


def test_text_encoding_fixes_macron_as_diaeresis_in_every_string_column():
    """Diaeresis characters standing in for macron vowels are fixed in every string column -
    not a dataset-specific list of them - while a non-string column and a null value pass
    through unchanged."""
    gdf = gpd.GeoDataFrame(
        {
            "full_text": pd.array(["Whakatäne", "Taupo", None], dtype="string"),
            "name": pd.array(["Pöhangina", "Taupo", None], dtype="string"),
            "text_colour": pd.array([9, 9, 9], dtype="Int32"),
        },
        geometry=[Point(0, 0), Point(1, 1), Point(2, 2)],
        crs="EPSG:2193",
    )
    out = text_encoding(gdf, _td([], "linz_carto_text"), 66)

    assert out["full_text"].tolist() == ["Whakatāne", "Taupo", pd.NA]
    assert out["name"].tolist() == ["Pōhangina", "Taupo", pd.NA]
    assert out["text_colour"].tolist() == [9, 9, 9]
