"""Dataset fixups: per-dataset, release-aware patches.

A fixup is an arbitrary function that takes a (already field-normalized)
GeoDataFrame plus the release id and returns the corrected frame. Use them for
one-off data repairs that can't be expressed declaratively in a theme's
`mapping` or `corrections` - setting a column on specific records, or dropping
records the source publishes broken (see `drop_degenerate_fences`).

Register a fixup by adding it to ``FIXUPS``; reference it from a dataset in the
theme config:

    fixups:
      - fn: drop_empty_residential_areas
        releases: [51]   # omit `releases` to apply to every release

Config loading validates that every referenced ``fn`` exists here, so a typo
fails at load time rather than mid-run. A fixup gated to a release that shares
its transform with an earlier one is rejected too, since it would never run;
gate it to that earlier release instead.

Prefer identifying records by a stable key (t50_fid) over anything positional,
and where a repair can be skipped safely - a record already fixed upstream -
gate on the data rather than the release, so a stale fixup fades out on its own
rather than corrupting a later snapshot.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from functools import cache
from typing import TYPE_CHECKING, NamedTuple

from .fixups_map_sheet import (
    map_sheet_drop_index_sheets,
    map_sheet_example_name_fixes,
    map_sheet_example_point_id,
    map_sheet_origin,
    map_sheet_published,
)

logger = logging.getLogger("kart_import")


if TYPE_CHECKING:
    import geopandas as gpd

    from kart_import.config import ThemeDataset

# (gdf, td, release_id) -> gdf
Fixup = Callable[["gpd.GeoDataFrame", "ThemeDataset", int], "gpd.GeoDataFrame"]


def _drop_listed_empty(
    gdf: gpd.GeoDataFrame,
    td: ThemeDataset,
    dataset: str,
    t50_fids: set[int],
) -> gpd.GeoDataFrame:
    """Drop the listed t50_fids of `dataset`, but only where the geometry is actually missing.

    The FlatGeobuf driver refuses an empty or NULL geometry while building a spatial index
    ("NULL geometry not supported with spatial index"), so one such feature fails the whole
    theme write.

    Gating on the geometry rather than the release is what lets the fid list be permanent: a
    fid whose geometry is present is kept and logged, so a feature repaired upstream is not
    deleted forever by a list nobody thought to trim.

    `dataset` names what the fid list was checked against. Wired to any other dataset the fids
    would simply match nothing, so the mistake would pass as a successful no-op build rather
    than an error.
    """
    import pandas as pd

    if td.name != dataset:
        raise ValueError(f"fixup for dataset '{dataset}' applied to '{td.name}'")

    fids = pd.to_numeric(gdf["t50_fid"], errors="coerce")
    listed = fids.isin(t50_fids)
    if not listed.any():
        return gdf

    # `isna` as well as `is_empty`: a NULL geometry fails the FlatGeobuf write the same way.
    missing = gdf.geometry.is_empty | gdf.geometry.isna()

    # Warn rather than drop: the fid is on the list but the source now carries a geometry for it,
    # so the list has outlived the defect and this fid can come off it.
    if (repaired := listed & ~missing).any():
        logger.warning(
            "listed fid now has geometry, keeping it",
            extra={"t50_fids": sorted(fids[repaired].astype(int).tolist())},
        )

    drop = listed & missing
    if not drop.any():
        return gdf

    logger.info(
        "dropping listed fids with no geometry",
        extra={"t50_fids": sorted(fids[drop].astype(int).tolist())},
    )
    return gdf[~drop].reset_index(drop=True)


def drop_degenerate_fences(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Drop four zero-length nz_fence_centrelines features.

    All four are the same 0.5 micrometre two-vertex line in EPSG:2193
    (1756000.000000001 5420267.181827734 -> ...182310526), present from the 2020-02-16 source
    snapshot on. Both vertices land in the same 1e-8 degree cell, so `set_precision` collapses
    them to LINESTRING EMPTY.

    Absent in releases 52-55 and back in 56+, hence the geometry gate rather than a release gate;
    see `_drop_listed_empty`.
    """
    return _drop_listed_empty(gdf, td, "nz_fence_centrelines", {7640059, 7640098, 7704786, 7704787})


def drop_empty_residential_areas(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Drop the one nz_residential_area_polygons feature the source publishes with no geometry.

    t50_fid 6753838 (Taihape): the 2020-11-17 snapshot (release 51) is the only one where
    the source contains a null geometry for this fid, and `kart export` reproduces that null verbatim.

    Gated to release 51 in the config. The fid is present in all 21 snapshots, so an ungated fixup
    would log the "now has geometry" warning on 20 of them.
    """
    return _drop_listed_empty(gdf, td, "nz_residential_area_polygons", {6753838})


def drop_degenerate_roads(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Drop two zero-length nz_road_centrelines features.

    Both are two-vertex lines whose vertices share a 1e-8 degree cell, so `set_precision`
    collapses them to LINESTRING EMPTY - the same defect as `drop_degenerate_fences`:
      6635943 "MINGAROA ROAD", releases 42-45, ~0.1mm apart in EPSG:2193
      8532247 "DEATHS ROAD", release 66, ~0.1mm apart

    Neither fid appears in any other release, so the geometry gate never fires the
    "now has geometry" warning and no release gate is needed; see `_drop_listed_empty`.
    """
    return _drop_listed_empty(gdf, td, "nz_road_centrelines", {6635943, 8532247})


class SourceRef(NamedTuple):
    """The constant half of a provenance record: which target column it explains, and where that
    column's value came from. Only `source_key_value` varies per row.

    Field names are the record's JSON keys.
    """

    table_column: str
    """The target column whose value this record accounts for."""
    source: str
    """The external system, e.g. `linz_aims`."""
    source_key_name: str
    """What that system calls the key, e.g. `road_id`."""
    source_table: str
    """The table within that system, e.g. `roads`."""
    source_column: str
    """The column within that table the value was read from."""
    source_updated_at: str | None = None
    """RFC 3339 UTC text, or `None` to stamp both timestamps from the release date - see
    `_build_source_metadata`. Set this only when a ref needs a different stamp, e.g. build-time
    wall-clock rather than the release it's imported for."""
    imported_at: str | None = None
    """As `source_updated_at`; the two are independent so a ref can set one without the other."""


ROAD_NAME_FROM_AIMS = SourceRef(
    table_column="name",
    source="linz_aims",
    source_key_name="road_id",
    source_table="roads",
    source_column="name",
)

ROAD_SUFI_UNSET = 0
"""`rna_sufi`'s "no AIMS road" sentinel. Never null in any release 30-66, always 0 - so this rather
than a null check is what separates a road whose name came from AIMS from one that has no link."""


def _rfc3339(moment: datetime) -> str:
    """`moment` as RFC 3339 UTC text (second precision, `Z` suffix) - the one timestamp shape every
    provenance record uses, whether stamped from a release date or the build's wall clock."""
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _release_stamp(release_id: int) -> str:
    """The release's own date as an RFC 3339 UTC stamp, for a provenance timestamp."""
    from .config import get_releases

    release_date = next((release.date for release in get_releases() if release.id == release_id), None)
    if release_date is None:
        raise LookupError(f"release {release_id} is not in the release config")
    # A naive date is read as UTC, not as the builder's local time: every entry in
    # `topo50_release.yml` carries a `Z`, so naive here means a test or a hand-built Release, and
    # `astimezone` on a naive value would otherwise make the output depend on the machine's TZ.
    if release_date.tzinfo is None:
        release_date = release_date.replace(tzinfo=UTC)
    return _rfc3339(release_date)


def _build_source_metadata(
    gdf: gpd.GeoDataFrame,
    td: ThemeDataset,
    release_id: int,
    dataset: str,
    ref: SourceRef,
    unset_key: int | None = None,
) -> gpd.GeoDataFrame:
    """Rewrite `metadata` from the raw external keys it is carrying into provenance records.

    A one-element JSON array of the shape the Postgres loader builds with `jsonb_build_array`.
    `metadata` is typed `string` in the theme schemas, so this is JSON *text*; the cast to `jsonb`
    happens downstream.

    **The column is its own input.** `normalize_fields` rebuilds the frame from the mapping alone,
    dropping every unmapped source column, and fixups run after it - so a fixup cannot reach the
    key column directly, and the key cannot be mapped to a column of its own.
    Instead, the config maps it *into* `metadata`:

        metadata: {source: $rna_sufi, fixup: true}

    and this rewrites those raw values into the finished records.

    `unset_key` is the source's "no link" sentinel, if it has one; rows holding it get NULL
    `metadata` rather than a record asserting a link to a row that does not exist. A NULL key is
    always treated as unset.

    A row whose looked-up value (`ref.table_column`) came back null also gets NULL `metadata`: a
    provenance record explains where that column's value came from, and a null value has nothing
    to explain.

    `dataset` names what the wiring was checked against, as in `_drop_listed_empty`: pointed at
    another dataset this would silently overwrite that dataset's `metadata` instead of failing.

    Keys are assumed numeric (`road_id` and NZGB's `feat_id` both are). A text-keyed source is
    where this would need to grow a coercion choice.
    """
    import json

    import pandas as pd

    if td.name != dataset:
        raise ValueError(f"fixup for dataset '{dataset}' applied to '{td.name}'")

    stamp = _release_stamp(release_id)

    # Not `errors="coerce"`: a non-numeric column here means the config wired something other than
    # the key column into `metadata`, which should fail rather than quietly produce an all-null one.
    key = pd.to_numeric(gdf["metadata"]).astype("Int32", errors="raise")
    has_key = key.notna() if unset_key is None else key.notna() & (key != unset_key)

    # Only a row whose looked-up value is present gets a record: the record explains where that
    # value came from, so a key that resolved to a null `ref.table_column` has nothing to explain
    # and gets NULL `metadata` instead of a record pointing at an absent value.
    named = gdf[ref.table_column].notna()
    keyed = has_key & named

    def record(key_value: int) -> str:
        # sort_keys/separators so the same key always serialises to the same bytes - two runs that
        # differ only in dict ordering would read as a changed feature to kart.
        return json.dumps(
            [
                {
                    **ref._asdict(),
                    "source_key_value": key_value,
                    "source_updated_at": ref.source_updated_at or stamp,
                    "imported_at": ref.imported_at or stamp,
                }
            ],
            sort_keys=True,
            separators=(",", ":"),
        )

    # Built per distinct key, not per row: one external record covers many features (21,602 of
    # release 66's 93,308 keyed roads repeat a sufi), and the record depends on nothing else.
    keys = key[keyed]
    records = {key_value: record(key_value) for key_value in keys.unique().tolist()}

    metadata = pd.Series(pd.NA, index=gdf.index, dtype="string")
    metadata[keyed] = keys.map(records).astype("string")

    logger.info(
        "built source metadata",
        extra={
            "dataset": td.name,
            "release": release_id,
            "source": ref.source,
            "keyed": int(keyed.sum()),
            "unlinked": int((~has_key).sum()),
            f"keyed_without_{ref.table_column}": int((has_key & ~named).sum()),
        },
    )

    gdf = gdf.copy()
    gdf["metadata"] = metadata
    return gdf


def build_road_metadata(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Record that `nz_road_centrelines`' `name` came from LINZ AIMS, keyed by `rna_sufi`."""
    return _build_source_metadata(
        gdf, td, release_id, "nz_road_centrelines", ROAD_NAME_FROM_AIMS, unset_key=ROAD_SUFI_UNSET
    )


def build_nzgb_metadata(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Record that a dataset's `name` came from the NZGB gazetteer, keyed by its lookup's key column.

    Generic across every dataset wired the way `nz_canal_polygons_topo_150k` is - a gazetteer
    lookup joined in, its name column mapped to `name`, and its key column mapped into `metadata`:

        name: $<lookup>.name
        metadata: {source: $<lookup>.<key column>, fixup: true}
        joins:
          - lookup: <lookup>
            left_on: t50_fid
        fixups:
          - fn: build_nzgb_metadata

    The lookup name, its key column, and its source dataset are all read off `td` and the theme's
    `lookups:`, so a new dataset needs none of the Python above - just that same wiring. A dataset
    whose gazetteer-sourced value lands somewhere other than `name` needs its own `SourceRef` and a
    direct `_build_source_metadata` call instead (see `build_road_metadata`).

    Both timestamps are stamped at build time rather than from the release date, unlike
    `build_road_metadata` - the gazetteer lookup carries no per-record update time of its own, so
    "when this ref was built" is the closest available proxy. Unlike a release-dated ref, this is
    not byte-stable: rebuilding the same release later reproduces different bytes for every row.
    """
    from .config import LOOKUP_MAP

    metadata_source = td.field_specs()["metadata"].source
    is_lookup_ref = isinstance(metadata_source, str) and metadata_source.startswith("$")
    lookup_name, sep, key_column = metadata_source[1:].partition(".") if is_lookup_ref else ("", "", "")
    if not sep or not key_column:
        raise ValueError(
            f"{td.name}: `metadata` must be mapped as '$<lookup>.<key column>' for "
            f"build_nzgb_metadata, got {metadata_source!r}"
        )

    name_source = td.field_specs()["name"].source
    if name_source != f"${lookup_name}.name":
        raise ValueError(
            f"{td.name}: `name` must be mapped as '${lookup_name}.name' to match the gazetteer "
            f"lookup '{lookup_name}' referenced by `metadata`, got {name_source!r}"
        )

    lookup = LOOKUP_MAP.get(lookup_name)
    if lookup is None:
        raise ValueError(f"{td.name}: `metadata` references unknown lookup '{lookup_name}'")

    build_stamp = _rfc3339(datetime.now(UTC))

    ref = SourceRef(
        table_column="name",
        source="nzgb_gazetteer",
        source_key_name="feat_id",
        source_table="nzgb_gaz",
        source_column="name",
        source_updated_at=build_stamp,
        imported_at=build_stamp,
    )
    return _build_source_metadata(gdf, td, release_id, td.name, ref)


def _split_id(parent_id: str, dataset_name: str, parent_fid, part_index: int) -> str:
    """Deterministic UUIDv7 for a part that has no source fid.

    Reuses the parent's 48-bit timestamp prefix (so the derived record sorts with the feature it
    came from) and hashes a key that cannot collide with a key from the lifecycle.
    """
    import uuid

    from .uuid7 import reproducable_uuid7_text

    try:
        timestamp_ms = int(uuid.UUID(str(parent_id)).hex[:12], 16)
    except ValueError:
        timestamp_ms = 0
    return str(reproducable_uuid7_text(timestamp_ms, f"{dataset_name}:{parent_fid}:part{part_index}"))


def split_multipart_features(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Explode multipart geometries into one record per part.

    The largest part stays on the original row (keeping its id, `t50_fid` and attributes); the
    remaining parts are appended as new records carrying the same attributes but a null `t50_fid`
    and a derived id. Single-part features are untouched, so a layer with no splits comes back
    unchanged.
    """
    import geopandas as gpd_
    import pandas as pd

    multipart = gdf.geometry.geom_type.isin(("MultiPolygon", "MultiLineString", "MultiPoint"))
    if not multipart.any():
        return gdf

    gdf = gdf.reset_index(drop=True)
    extra_rows = []

    for position in gdf.index[multipart.to_numpy()]:
        row = gdf.loc[position]
        parts = sorted(row.geometry.geoms, key=lambda part: part.area or part.length, reverse=True)
        parent_fid = row["t50_fid"] if "t50_fid" in gdf.columns else None

        # The largest part keeps the source feature's identity.
        gdf.at[position, "geometry"] = parts[0]

        for part_index, part in enumerate(parts[1:], start=1):
            new_row = row.copy()
            new_row["geometry"] = part
            new_row["id"] = _split_id(row["id"], td.name, parent_fid, part_index)
            if "t50_fid" in gdf.columns:
                new_row["t50_fid"] = pd.NA
            extra_rows.append(new_row)

        logger.info(
            "split multipart feature",
            extra={
                "dataset": td.name,
                "release": release_id,
                "t50_fid": None if parent_fid is None or pd.isna(parent_fid) else int(parent_fid),
                "parts": len(parts),
                "kept": float(parts[0].area or parts[0].length),
                "split_off": [float(p.area or p.length) for p in parts[1:]],
            },
        )

    if not extra_rows:
        return gdf

    added = gpd_.GeoDataFrame(extra_rows, columns=gdf.columns, crs=gdf.crs).set_geometry("geometry")
    out = pd.concat([gdf, added], ignore_index=True)
    return gpd_.GeoDataFrame(out, geometry="geometry", crs=gdf.crs)


def drop_degenerate_tracks(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Drop the zero-length nz_track_centrelines features."""
    return _drop_listed_empty(
        gdf,
        td,
        "nz_track_centrelines",
        {
            7708693,
            8118480,
            8337121,
            8337123,
            8337124,
            8337127,
            8337128,
            8337131,
            8337324,
            8337326,
            8337327,
            8337329,
            8337330,
            8337331,
            8494016,
            8494018,
            8511453,
            8511454,
            8511455,
            8511457,
            8511468,
            8511469,
            8532815,
            8532817,
            8532818,
            8532821,
            8532822,
            8532824,
            8532825,
        },
    )


def contour_number(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """LAMPS orientation is contour tangent in radians, counter-clockwise from east.

    LAMPS cont_rota is perpendicular to the contour and points downhill."""
    import numpy as np

    bearing = 90 - np.degrees(gdf["orientation"])
    flip = ((gdf["cont_rota"] - bearing) % 360) > 180
    bearing = np.where(flip, bearing + 180, bearing)
    gdf["orientation"] = np.round(bearing - 90) % 360
    gdf["orientation"] = gdf["orientation"].astype("Int32")

    gdf = gdf.drop(columns="cont_rota")

    gdf["label"] = gdf["label"].astype("Int64").astype("string")
    return gdf

CARTO_TEXT_COLOUR: dict[int, str] = {9: "black", 5: "warm_red", 6: "process_blue"}
CARTO_TEXT_COLOUR_DEFAULT = "black"
"""`text_colour` integer code -> `colour` enum. Legacy decoded any other code to black."""

CARTO_TEXT_FONT = "Nimbus Sans LINZ"
CARTO_TEXT_STYLE: dict[str, str] = {
    "ATTriumMou-Regular": "Regular",
    "ATTriumMou-Cond": "Narrow",
    "ATTriumMou-CondBold": "Narrow Bold",
    "ATTriumMou-CondItalic": "Narrow Italic",
    "ATTriumMou-Italic": "Italic",
    "ATTrium-Italic": "Regular",
    "Courier Bold Oblique": "Regular",
}
"""mapping from source `text_font` values to `style` enum."""

def _carto_text_key(bend, height, place, style, colour):
    """Join the five key columns into one string, e.g. `0|67.0000|31|Narrow|black` for looks up mapping from csv file."""
    import pandas as pd

    def as_text(series, decimals):
        return series.astype("Float64").map(lambda v: f"{v:.{decimals}f}" if pd.notna(v) else pd.NA).astype("string")

    parts = [
        as_text(bend, 0),
        as_text(height, 4),
        as_text(place, 0),
        style.astype("string"),
        colour.astype("string"),
    ]

    key = parts[0]
    for part in parts[1:]:
        key = key.str.cat(part, sep="|", na_rep="\x00")
    return key


# Columns filled from the carto_text_styling table split by output dtype.
_CARTO_TEXT_STRING_FIELDS = ("placement", "textanchor", "charplace")
_CARTO_TEXT_NUMBER_FIELDS = ("size", "offset", "labelanchor", "chardistance")
_CARTO_TEXT_KEY_INPUTS = ("text_bend", "text_height", "text_placement", "text_colour", "text_font")

@cache
def _carto_text_styling_table():
    """Read the carto_text_styling table from the carto_text_styling.csv file and prepare it for lookups."""
    import pandas as pd

    from .config import CONFIG_DIR_CARTO_TEXT_STYLING

    df = pd.read_csv(CONFIG_DIR_CARTO_TEXT_STYLING)
    for column in _CARTO_TEXT_STRING_FIELDS + ("style", "colour"):
        df[column] = df[column].astype("string").str.strip()
    for column in _CARTO_TEXT_NUMBER_FIELDS:
        df[column] = pd.to_numeric(df[column]).astype("Float64")
    df["key"] = _carto_text_key(df["text_bend"], df["text_height"], df["text_placement"], df["style"], df["colour"])
    if df["key"].duplicated().any():
        raise ValueError("carto_text_styling.csv has duplicate composite keys")
    return df.set_index("key")


def carto_text_styling(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Fill the cartographic styling fields for carto_text based on the mapping from carto_text_styling.csv.
    """
    import pandas as pd

    missing = set(_CARTO_TEXT_KEY_INPUTS) - set(gdf.columns)
    if missing:
        raise ValueError(f"carto_text_styling needs columns {sorted(missing)}, absent from '{td.name}'")

    table = _carto_text_styling_table()

    colour = gdf["text_colour"].map(CARTO_TEXT_COLOUR).astype("string").fillna(CARTO_TEXT_COLOUR_DEFAULT)
    style = gdf["text_font"].map(CARTO_TEXT_STYLE).astype("string")
    key = _carto_text_key(gdf["text_bend"], gdf["text_height"], gdf["text_placement"], style, colour)
    matched = key.isin(table.index)

    gdf = gdf.copy()
    gdf["colour"] = colour.where(matched)
    gdf["style"] = style.where(matched)
    gdf["font"] = pd.Series(CARTO_TEXT_FONT, index=gdf.index, dtype="string").where(matched)
    for column in _CARTO_TEXT_STRING_FIELDS:
        gdf[column] = key.map(table[column]).astype("string")
    for column in _CARTO_TEXT_NUMBER_FIELDS:
        gdf[column] = pd.to_numeric(key.map(table[column]), errors="coerce").astype("Float64")

    logger.info(
        "carto_text_styling",
        extra={"dataset": td.name, "release": release_id, "matched": int(matched.sum()), "total": len(gdf)},
    )
    return gdf


def _grid_values(low: float, high: float, interval: float) -> list[float]:
    """Grid line positions covering ``low``..``high``, snapped outward to whole intervals."""
    import math

    first = math.floor(low / interval)
    last = math.ceil(high / interval)
    return [(first + i) * interval for i in range(last - first + 1)]


def _grid_line(value: float, along_x: bool, low: float, high: float, vertices: int):
    """One grid line of ``vertices`` evenly spaced points, always wound low to high: along x holding
    y ``value`` when ``along_x``, else along y holding x ``value``."""
    from shapely.geometry import LineString

    span = [low + (high - low) * i / (vertices - 1) for i in range(vertices)]
    if along_x:
        return LineString([(s, value) for s in span])
    return LineString([(value, s) for s in span])


def _generate_grid_features(
    gdf: gpd.GeoDataFrame,
    td: ThemeDataset,
    release_id: int,
    *,
    bounds: tuple[float, float, float, float],
    directions: tuple[str, str],
    interval: float,
    crs: str,
    vertices: int,
    margin: int = 0,
) -> gpd.GeoDataFrame:
    """Create grid with lines for a map sheet.

    ``bounds`` is the extent to rule, as ``(minx, miny, maxx, maxy)`` already in ``crs`` units, and
    ``directions`` labels the two line families, as (along x, along y).
    Lines are ruled every ``interval`` units of ``crs``, each drawn with ``vertices``
    evenly spaced points so it follows the curve of the grid once reprojected.
    """
    import geopandas as gpd_

    from .uuid7 import reproducable_uuid7_text

    minx, miny, maxx, maxy = bounds
    pad = margin * interval
    minx, miny, maxx, maxy = minx - pad, miny - pad, maxx + pad, maxy + pad
    xs = _grid_values(minx, maxx, interval)
    ys = _grid_values(miny, maxy, interval)
    along_x, along_y = directions

    # Lines run edge to edge of the snapped extent, so every sheet is ruled corner to corner.
    lines = [(along_x, value, _grid_line(value, True, xs[0], xs[-1], vertices)) for value in ys]
    lines += [(along_y, value, _grid_line(value, False, ys[0], ys[-1], vertices)) for value in xs]

    # IDs are a pure function of the line itself based on the earliest release timestamp (release 30)
    base_timestamp = int(datetime(2015, 11, 19, 2, 33, 32, tzinfo=UTC).timestamp() * 1000)
    ids = [
        str(reproducable_uuid7_text(base_timestamp, f"{td.name}:{direction}:{value:.9f}"))
        for direction, value, _ in lines
    ]

    logger.info(
        "generate_grid_features",
        extra={
            "dataset": td.name,
            "release": release_id,
            "lines": len(lines),
            "extent": [minx, miny, maxx, maxy],
        },
    )

    geoms = gpd_.GeoSeries([geom for _, _, geom in lines], crs=crs)
    if gdf.crs is not None:
        geoms = geoms.to_crs(gdf.crs)
    return gpd_.GeoDataFrame(
        {
            "id": ids,
            "t50_fid": [None] * len(lines),
            "direction": [direction for direction, _, _ in lines],
            "value": [value for _, value, _ in lines],
        },
        geometry=geoms.reset_index(drop=True),
        crs=geoms.crs,
    )


def generate_nztm_grid_features(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Rule the nztopo50_grid: 1 km lines in NZTM2000, straight in projection so two points are sufficient"""
    return _generate_grid_features(
        gdf,
        td,
        release_id,
        bounds=(1_084_000, 4_722_000, 2_092_000, 6_234_000),
        directions=("easting", "northing"),
        interval=1_000,
        crs="EPSG:2193",
        vertices=2,
        margin=1,
    )


def generate_dms_grid_features(gdf: gpd.GeoDataFrame, td: ThemeDataset, release_id: int) -> gpd.GeoDataFrame:
    """Rule the nztopo50_dms_grid: one arcminute lines in WGS84, 1000 vertices so they stay curved when reprojected into NZTM2000."""
    return _generate_grid_features(
        gdf,
        td,
        release_id,
        bounds=(166, -48, 180, -34),
        directions=("longitude", "latitude"),
        interval=1.0 / 60.0,
        crs="EPSG:4326",
        vertices=1_001,
        margin=0,
    )


FIXUPS: dict[str, Fixup] = {
    "build_nzgb_metadata": build_nzgb_metadata,
    "build_road_metadata": build_road_metadata,
    "carto_text_styling": carto_text_styling,
    "drop_degenerate_fences": drop_degenerate_fences,
    "drop_degenerate_roads": drop_degenerate_roads,
    "drop_degenerate_tracks": drop_degenerate_tracks,
    "drop_empty_residential_areas": drop_empty_residential_areas,
    "generate_dms_grid_features": generate_dms_grid_features,
    "generate_nztm_grid_features": generate_nztm_grid_features,
    "split_multipart_features": split_multipart_features,
    "contour_number": contour_number,
    "map_sheet_drop_index_sheets": map_sheet_drop_index_sheets,
    "map_sheet_origin": map_sheet_origin,
    "map_sheet_example_name_fixes": map_sheet_example_name_fixes,
    "map_sheet_example_point_id": map_sheet_example_point_id,
    "map_sheet_published": map_sheet_published,
}
