import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, Polygon

from . import fixups_map_sheet
from .assets import transform
from .config import ThemeDataset


def _td(fixups_cfg: list[dict], name: str = "t") -> ThemeDataset:
    return ThemeDataset.model_validate(
        {"name": name, "source": "kart@data.koordinates.com:linz/x-topo-150k", "fixups": fixups_cfg}
    )


def test_map_sheet_origin_uses_top_left_corner():
    """origin_x/origin_y are the sheet polygon's top-left corner (minx, maxy), as nullable floats."""
    gdf = gpd.GeoDataFrame(
        {"sheet_code": ["BK37"]},
        geometry=[Polygon([(1876000, 5586000), (1900000, 5586000), (1900000, 5622000), (1876000, 5622000)])],
        crs="EPSG:2193",
    )
    out = fixups_map_sheet.map_sheet_origin(gdf, _td([], name="linz_map_sheet"), 66)
    assert out["origin_x"].tolist() == [1876000.0]
    assert out["origin_y"].tolist() == [5622000.0]
    assert str(out["origin_x"].dtype) == "Float64"


def test_map_sheet_published_matches_current_edition(tmp_path, monkeypatch):
    """`published_version` is parsed from the edition; `published_at`/`updated_at` come from the
    per-sheet edition history matched on the sheet's current edition."""
    from . import config

    (tmp_path / "map_sheet_published.yml").write_text(
        'BK37:\n  "1.05": "2023-12-19T21:07:28Z"\n  "1.06": "2025-08-13T22:08:55Z"\n'
    )
    monkeypatch.setattr(config, "CONFIG_DIR", tmp_path)

    gdf = gpd.GeoDataFrame(
        {
            "sheet_code": ["BK37", "BK37"],
            "published_version": ["Edition 1.05 Published 2023", "Edition 9.99 Published 2099"],
        },
        geometry=[Point(0, 0), Point(1, 1)],
        crs="EPSG:2193",
    )
    out = fixups_map_sheet.map_sheet_published(gdf, _td([], name="linz_map_sheet"), 66)
    assert out["published_version"].tolist() == ["1.05", "9.99"]
    # 1.05 matched exactly; 9.99 is absent -> falls back to the sheet's latest edition (1.06)
    assert out["published_at"].tolist() == ["2023-12-19T21:07:28Z", "2025-08-13T22:08:55Z"]
    assert out["updated_at"].equals(out["published_at"])


def test_map_sheet_example_point_id_resolves_by_class(monkeypatch):
    """Each sheet's example point resolves to a feature id: a trig_pnt via the trig_point theme
    (matched on `code`), anything else via the geographic_name theme (matched on `name`)."""
    from types import SimpleNamespace

    from . import config

    monkeypatch.setattr(
        config, "get_theme_by_name", lambda name: SimpleNamespace(datasets=[SimpleNamespace(name=name)])
    )

    def fake_read_transform(path):
        if "trig_point" in str(path):
            return pd.DataFrame({"code": ["AB01"], "id": ["trig-id"]})
        return pd.DataFrame({"name": ["Lake Tekapo"], "id": ["name-id"]})

    monkeypatch.setattr(transform, "read_transform", fake_read_transform)

    gdf = gpd.GeoDataFrame(
        {"example_name": ["AB01", "Lake Tekapo"], "example_class": ["trig_pnt", "geographic_name"]},
        geometry=[Point(0, 0), Point(1, 1)],
        crs="EPSG:2193",
    )
    out = fixups_map_sheet.map_sheet_example_point_id(gdf, _td([], name="linz_map_sheet"), 66)
    assert out["example_point_id"].tolist() == ["trig-id", "name-id"]
    assert "example_name" not in out.columns
    assert "example_class" not in out.columns


def test_map_sheet_drop_index_sheets():
    """Index sheets (sheet_code starting 'Topo') are dropped; real sheets kept, index reset."""
    gdf = gpd.GeoDataFrame(
        {"sheet_code": ["BK37", "TopoBDE00", "CB10", "TopoBDE01"]},
        geometry=[Point(i, i) for i in range(4)],
        crs="EPSG:2193",
    )
    out = fixups_map_sheet.map_sheet_drop_index_sheets(gdf, _td([], name="linz_map_sheet"), 66)
    assert out["sheet_code"].tolist() == ["BK37", "CB10"]
    assert out.index.tolist() == [0, 1]


def test_map_sheet_example_name_fixes():
    """example_name is corrected before the example_point_id lookup: Mt->Mount, trig code remaps,
    macron restorations; other names pass through untouched."""
    gdf = gpd.GeoDataFrame(
        {"example_name": ["Mt Ararat", "A0TR", "AP8Y", "Putata", "Pohoi", "Rahuimokairoa", "Wellington"]},
        geometry=[Point(i, i) for i in range(7)],
        crs="EPSG:2193",
    )
    out = fixups_map_sheet.map_sheet_example_name_fixes(gdf, _td([], name="linz_map_sheet"), 66)
    assert out["example_name"].tolist() == [
        "Mount Ararat",
        "A0U2",
        "A4UX",
        "Pūtata",
        "Pōhoi",
        "Rāhuimōkairoa",
        "Wellington",
    ]
