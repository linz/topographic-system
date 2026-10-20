from __future__ import annotations

import logging
from datetime import UTC
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import geopandas as gpd

    from kart_import.config import ThemeDataset

logger = logging.getLogger("kart_import")


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
      - macron restorations Putata->Pūtata, Pohoi->Pōhoi, Rahuimokairoa->Rāhuimōkairoa
    Gazetteer renames, which only hold from some release on, are in `GAZETTEER_RENAMES`."""
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


# Gazetteer renames, old name -> new. Releases from before a rename still carry the old name.
GAZETTEER_RENAMES = {
    "Mount Treacey": "Mount Treacy",  # release 67
}


def gazetteer_name(name: str, known) -> str:
    """`name` if `known` has it, else its gazetteer rename (if any)."""
    return name if name in known else GAZETTEER_RENAMES.get(name, name)


def map_sheet_example_point_id(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    from .assets.export import backfilled_release_ids
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
        match = lookup.get(gazetteer_name(example_name, lookup))
        if match is None:
            unmatched.append((sheet_code, example_class, example_name))
        example_point_id.append(match)

    if unmatched:
        detail = ", ".join(f"{code} ({cls}: {name!r})" for code, cls, name in unmatched)
        if td.backfill and release_id in backfilled_release_ids(td.name):
            # The example names are from the earliest source commit; this release's trig points and
            # geographic names can predate them, so leave the link empty rather than fail.
            logger.warning(
                "backfilled map sheet example_name has no match, example_point_id left empty",
                extra={"dataset": td.name, "release": release_id, "unmatched": detail},
            )
        else:
            raise ValueError(
                f"{td.name}: {len(unmatched)} map sheet(s) have an example_name with no matching "
                f"trig_point/geographic_name feature - add corrections to map_sheet_example_name_fixes: {detail}"
            )

    gdf["example_point_id"] = example_point_id
    gdf = gdf.drop(columns=["example_name", "example_class"])
    return gdf


def map_sheet_published(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Set `published_version` from the source `edition`, and `published_at`/`updated_at` from the
    per-sheet edition history in `config/map_sheet_published.yml`. `created_at` is the sheet's first
    edition, in place of when it first appeared in the source repo.

    A sheet whose edition was published after this release (a `backfill` release carrying a later
    export) is rewound to the latest edition published by the release's date. A sheet with no
    edition that early keeps its earliest known one."""
    import yaml

    from .config import CONFIG_DIR, get_releases

    edition = gdf["published_version"]
    gdf["published_version"] = edition.str.extract(r"Edition\s+([0-9]+(?:\.[0-9]+)?)", expand=False)

    with open(CONFIG_DIR / "map_sheet_published.yml") as f:
        history = yaml.safe_load(f)

    release = next((r for r in get_releases() if r.id == release_id), None)
    cutoff = release.date.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ") if release else None
    too_early = []

    def pick(row):
        versions = history.get(row["sheet_code"])
        if not versions:
            return row["published_version"], None
        version = row["published_version"]
        published_at = versions.get(version) or max(versions.values())
        if cutoff is None or published_at <= cutoff:
            return version, published_at
        earlier = [(at, ed) for ed, at in versions.items() if at <= cutoff]
        if not earlier:
            too_early.append(row["sheet_code"])
            earlier = [min((at, ed) for ed, at in versions.items())]
        published_at, version = max(earlier)
        return version, published_at

    picked = [pick(row) for _, row in gdf.iterrows()]
    if too_early:
        logger.warning(
            "map sheet has no edition published by this release, kept its earliest",
            extra={"dataset": td.name, "release": release_id, "sheets": too_early},
        )
    published_at = [at for _, at in picked]
    gdf["published_version"] = [version for version, _ in picked]
    gdf["published_at"] = published_at
    gdf["updated_at"] = published_at
    created_at = gdf["created_at"] if "created_at" in gdf.columns else [None] * len(gdf)
    gdf["created_at"] = [
        min(history[code].values()) if history.get(code) else at
        for code, at in zip(gdf["sheet_code"], created_at, strict=True)
    ]
    return gdf
