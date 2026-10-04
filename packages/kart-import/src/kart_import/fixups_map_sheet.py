from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import geopandas as gpd

    from kart_import.config import ThemeDataset


def map_sheet_origin(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    bounds = gdf.geometry.bounds
    gdf["origin_x"] = bounds["minx"].round(0).astype("Float64")
    gdf["origin_y"] = bounds["maxy"].round(0).astype("Float64")
    return gdf


def map_sheet_drop_index_sheets(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Drop the whole-country / whole-island index sheets, whose `sheet_code` starts with "Topo"
    (TopoBDE00/01/02 = "50k New Zealand / North Island / South Island")."""
    keep = ~gdf["sheet_code"].astype("string").str.startswith("Topo", na=False)
    return gdf[keep].reset_index(drop=True)


def map_sheet_example_name_fixes(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Correct `example_name` so it matches the trig_point/geographic_name lookups used by
    `map_sheet_example_point_id` (which must run *after* this fixup). Three classes of fix:
      - "Mt X" -> "Mount X"  (geographic names are stored with the full word)
      - trig code remaps A0TR->A0U2, AP8Y->A4UX
      - macron restorations Putata->Pūtata, Pohoi->Pōhoi, Rahuimokairoa->Rāhuimōkairoa"""
    names = gdf["example_name"].astype("string").str.replace(r"^Mt\s+", "Mount ", regex=True)
    names = names.replace(
        {
            "A0TR": "A0U2",
            "AP8Y": "A4UX",
            "Putata": "Pūtata",
            "Pohoi": "Pōhoi",
            "Rahuimokairoa": "Rāhuimōkairoa",
        }
    )
    gdf = gdf.copy()
    gdf["example_name"] = names
    return gdf


def map_sheet_example_point_id(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    from .assets.transform import read_transform
    from .config import TRANSFORM_SUFFIX, WORKING_TRANSFORM_DIR, get_theme_by_name

    trig_lookup = {}
    for dataset in get_theme_by_name("trig_point").datasets:
        frame = read_transform(WORKING_TRANSFORM_DIR / f"release_{release_id}" / f"{dataset.name}{TRANSFORM_SUFFIX}")
        for code, id in zip(frame["code"], frame["id"], strict=True):
            trig_lookup[code] = id

    geographic_name_lookup = {}
    for dataset in get_theme_by_name("geographic_name").datasets:
        frame = read_transform(WORKING_TRANSFORM_DIR / f"release_{release_id}" / f"{dataset.name}{TRANSFORM_SUFFIX}")
        for name, id in zip(frame["name"], frame["id"], strict=True):
            geographic_name_lookup[name] = id

    example_point_id = []
    unmatched = []
    sheet_codes = gdf["sheet_code"] if "sheet_code" in gdf.columns else gdf.index.astype(str)
    for sheet_code, example_name, example_class in zip(
        sheet_codes, gdf["example_name"], gdf["example_class"], strict=True
    ):
        lookup = trig_lookup if example_class == "trig_pnt" else geographic_name_lookup
        match = lookup.get(example_name)
        if match is None:
            unmatched.append((sheet_code, example_class, example_name))
        example_point_id.append(match)

    if unmatched:
        detail = ", ".join(f"{code} ({cls}: {name!r})" for code, cls, name in unmatched)
        raise ValueError(
            f"{td.name}: {len(unmatched)} map sheet(s) have an example_name with no matching "
            f"trig_point/geographic_name feature - add corrections to map_sheet_example_name_fixes: {detail}"
        )

    gdf["example_point_id"] = example_point_id
    gdf = gdf.drop(columns=["example_name", "example_class"])
    return gdf


def map_sheet_published(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Set `published_version` from the source `edition`, and `published_at`/`updated_at` from the
    per-sheet edition history in `config/map_sheet_published.yml`"""
    import yaml

    from .config import CONFIG_DIR

    edition = gdf["published_version"]
    gdf["published_version"] = edition.str.extract(r"Edition\s+([0-9]+(?:\.[0-9]+)?)", expand=False)

    with open(CONFIG_DIR / "map_sheet_published.yml") as f:
        history = yaml.safe_load(f)

    def pick(row):
        versions = history.get(row["sheet_code"])
        if not versions:
            return None
        return versions.get(row["published_version"]) or max(versions.values())

    published_at = gdf.apply(pick, axis=1)
    gdf["published_at"] = published_at
    gdf["updated_at"] = published_at
    return gdf
