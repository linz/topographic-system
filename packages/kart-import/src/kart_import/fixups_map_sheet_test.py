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
    assert out["created_at"].tolist() == ["2016-10-26T08:24:09Z"]


def test_map_sheet_published_sets_created_at_to_the_first_edition(monkeypatch, tmp_path):
    """created_at is when the sheet was first published, the same whichever edition a release has;
    a sheet with no edition history keeps the created_at it came in with."""
    from datetime import UTC, datetime

    from . import config

    (tmp_path / "map_sheet_published.yml").write_text(
        'BK37:\n  "1.04": "2016-10-26T08:24:09Z"\n  "1.06": "2023-08-18T00:30:50Z"\n'
    )
    monkeypatch.setattr(config, "CONFIG_DIR", tmp_path)
    monkeypatch.setattr(config, "get_releases", lambda: [config.Release(id=66, date=datetime(2026, 4, 23, tzinfo=UTC))])
    gdf = gpd.GeoDataFrame(
        {
            "sheet_code": ["BK37", "ZZ99"],
            "published_version": ["Edition 1.06 Published 2023", "Edition 1.00 Published 2026"],
            "created_at": ["2026-05-15T00:00:00Z", "2026-05-15T00:00:00Z"],
        },
        geometry=[Point(0, 0), Point(1, 1)],
        crs="EPSG:2193",
    )
    out = fixups_map_sheet.map_sheet_published(gdf, _td([], name="linz_map_sheet"), 66)

    assert out["published_at"].tolist() == ["2023-08-18T00:30:50Z", None]
    assert out["created_at"].tolist() == ["2016-10-26T08:24:09Z", "2026-05-15T00:00:00Z"]


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
        {
            "sheet_code": ["BK37", "BK38"],
            "example_name": ["AB01", "Lake Tekapo"],
            "example_class": ["trig_pnt", "geographic_name"],
        },
        geometry=[Point(0, 0), Point(1, 1)],
        crs="EPSG:2193",
    )
    out = fixups_map_sheet.map_sheet_example_point_id(gdf, _td([], name="linz_map_sheet"), 66)
    assert out["example_point_id"].tolist() == ["trig-id", "name-id"]
    assert "example_name" not in out.columns
    assert "example_class" not in out.columns


def test_map_sheet_example_point_id_raises_on_no_match(monkeypatch):
    from types import SimpleNamespace

    import pytest

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
        {
            "sheet_code": ["BK37", "BK38"],
            # BK38's name is absent from the geographic_name lookup -> no match
            "example_name": ["AB01", "Nowhere"],
            "example_class": ["trig_pnt", "geographic_name"],
        },
        geometry=[Point(0, 0), Point(1, 1)],
        crs="EPSG:2193",
    )
    with pytest.raises(ValueError, match=r"BK38 \(geographic_name: 'Nowhere'\)") as exc:
        fixups_map_sheet.map_sheet_example_point_id(gdf, _td([], name="linz_map_sheet"), 66)
    assert "BK37" not in str(exc.value)


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


def _published(monkeypatch, tmp_path, release_id: int, sheets: dict[str, str]) -> gpd.GeoDataFrame:
    """Run `map_sheet_published` for one release dated 2020-06-01 against a small edition history."""
    from datetime import UTC, datetime

    from . import config

    (tmp_path / "map_sheet_published.yml").write_text(
        'BK37:\n  "1.04": "2016-10-26T08:24:09Z"\n  "1.05": "2020-08-03T02:06:46Z"\n  "1.06": "2023-08-18T00:30:50Z"\n'
        'BW18:\n  "1.03": "2021-01-01T00:00:00Z"\n'
    )
    monkeypatch.setattr(config, "CONFIG_DIR", tmp_path)
    monkeypatch.setattr(config, "get_releases", lambda: [config.Release(id=50, date=datetime(2020, 6, 1, tzinfo=UTC))])
    gdf = gpd.GeoDataFrame(
        {"sheet_code": list(sheets), "published_version": list(sheets.values())},
        geometry=[Point(i, i) for i in range(len(sheets))],
        crs="EPSG:2193",
    )
    return fixups_map_sheet.map_sheet_published(gdf, _td([], name="linz_map_sheet"), release_id)


def test_map_sheet_published_rewinds_a_later_edition_to_the_release(monkeypatch, tmp_path):
    """A backfilled release carries a later export: BK37 1.06 (2023) is rewound to 1.04, the latest
    edition published by the release's 2020-06-01 date (1.05 came out in August)."""
    out = _published(monkeypatch, tmp_path, 50, {"BK37": "Edition 1.06 Published 2023"})

    assert out["published_version"].tolist() == ["1.04"]
    assert out["published_at"].tolist() == ["2016-10-26T08:24:09Z"]
    assert out["updated_at"].equals(out["published_at"])


def test_map_sheet_published_keeps_the_earliest_edition_when_none_predate_the_release(monkeypatch, tmp_path):
    """BW18's first known edition postdates the release: the sheet is kept at that edition, not dropped."""
    out = _published(monkeypatch, tmp_path, 50, {"BW18": "Edition 1.03 Published 2021"})

    assert out["published_version"].tolist() == ["1.03"]
    assert out["published_at"].tolist() == ["2021-01-01T00:00:00Z"]


def test_map_sheet_published_leaves_an_unknown_release_alone(monkeypatch, tmp_path):
    """No release date to rewind to: the source edition stands."""
    out = _published(monkeypatch, tmp_path, 99, {"BK37": "Edition 1.06 Published 2023"})

    assert out["published_version"].tolist() == ["1.06"]
    assert out["published_at"].tolist() == ["2023-08-18T00:30:50Z"]


def test_gazetteer_name_only_renames_when_the_old_name_is_missing():
    """Releases before a gazetteer rename still carry the old name, so it is kept while present."""
    assert fixups_map_sheet.gazetteer_name("Mount Treacey", {"Mount Treacey"}) == "Mount Treacey"
    assert fixups_map_sheet.gazetteer_name("Mount Treacey", {"Mount Treacy"}) == "Mount Treacy"
    assert fixups_map_sheet.gazetteer_name("Lake Tekapo", set()) == "Lake Tekapo"
