"""
Build the sea (moana) polygons for the water layer from the derived land polygons.

The land polygons (produced by ``coastline_polygon.py``) are inverted to produce
the sea. To avoid a single large polygon, the sea is sliced by the Web Mercator
quadkey grid (WebMercatorQuad) at a fixed zoom level: each tile contributes one
``type = moana`` water feature clipped to that tile's extent.
"""

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

import geopandas as gpd
import morecantile
import pandas as pd
import shapely
from pyproj import CRS

from data_prep.identity import earliest_created_at, reproducible_uuid7
from data_prep.parquet_utils import NZGD2000, WEB_MERCATOR, read_and_project, write_parquet

logger = logging.getLogger(__name__)

# WebMercatorQuad tile matrix set (EPSG:3857) provides the Web Mercator quadkey
# grid used to slice the sea into tile-sized extents.
TILE_MATRIX_SET = morecantile.tms.get("WebMercatorQuad")

# Web Mercator quadkey zoom level used to slice the sea into tile-sized extents.
SLICE_ZOOM = 7

# Output properties for the sea polygons written into the water layer.
OUTPUT_COLUMNS = [
    "id",
    "quadkey",
    "type",
    "updated_at",
    "created_at",
    "geometry",
]


def land_to_sea_tiles(land_gdf: gpd.GeoDataFrame, zoom: int) -> gpd.GeoDataFrame:
    """Invert the land polygons into per-tile sea polygons.

    Each Web Mercator quadkey tile overlapping the land extent yields one sea
    polygon (the tile with the land subtracted). Tiles fully covered by land are
    dropped.
    """
    land_crs = land_gdf.crs
    assert land_crs is not None, "land polygons must have a CRS"
    web_gdf = land_gdf.to_crs(WEB_MERCATOR)
    land_union = web_gdf.geometry.union_all()

    minx, miny, maxx, maxy = land_gdf.total_bounds
    geographic_crs = CRS.from_epsg(NZGD2000)

    tiles = list(TILE_MATRIX_SET.tiles(minx, miny, maxx, maxy, zooms=[zoom], geographic_crs=geographic_crs))

    xs: list[int] = []
    ys: list[int] = []
    geometries: list[shapely.Geometry] = []
    for tile in tiles:
        bounds = TILE_MATRIX_SET.xy_bounds(tile)
        tile_geom = shapely.box(bounds.left, bounds.bottom, bounds.right, bounds.top)
        sea = tile_geom.difference(land_union) if tile_geom.intersects(land_union) else tile_geom
        if sea.is_empty:
            continue
        xs.append(tile.x)
        ys.append(tile.y)
        geometries.append(sea)

    sea_gdf = gpd.GeoDataFrame({"x": xs, "y": ys}, geometry=geometries, crs=WEB_MERCATOR)
    return sea_gdf.to_crs(land_crs)


def set_derived_identity(
    sea_gdf: gpd.GeoDataFrame, zoom: int, source_created_at: datetime, produced_at: datetime
) -> gpd.GeoDataFrame:
    """Assign the quadkey, a reproducible id and timestamps to each sea polygon.

    The quadkey is derived from each tile's ``x``/``y`` index. The UUIDv7
    timestamp and ``created_at`` come from the source land polygons' earliest
    ``created_at`` so ids stay stable across reruns. A tile whose sea is
    disconnected is split into several single-part polygons that share a quadkey,
    so a per-tile part index is folded into the id seed to keep ids unique and
    reproducible. ``updated_at`` is the produce time.
    """
    result = sea_gdf.copy()

    result["quadkey"] = [
        TILE_MATRIX_SET.quadkey(morecantile.Tile(x, y, zoom)) for x, y in zip(result["x"], result["y"], strict=True)
    ]
    timestamp_ms = int(pd.Timestamp(source_created_at).timestamp() * 1000)
    # Several single-part polygons can share a quadkey; a per-tile part index
    # keeps each id unique while staying reproducible across reruns.
    part_index = result.groupby("quadkey", sort=False).cumcount()
    result["id"] = [
        str(reproducible_uuid7(timestamp_ms, f"{quadkey}:{part}"))
        for quadkey, part in zip(result["quadkey"], part_index, strict=True)
    ]
    result["type"] = "moana"
    result["created_at"] = source_created_at.isoformat()
    result["updated_at"] = produced_at.isoformat()
    return result


def run(coastline_path: Path, output_path: Path, zoom: int = SLICE_ZOOM) -> None:
    land_gdf = read_and_project(coastline_path, target_crs=NZGD2000)

    produced_at = datetime.now(ZoneInfo("Pacific/Auckland"))
    # Use the earliest source created_at so derived ids stay stable across reruns.
    source_created_at = earliest_created_at(land_gdf)

    # Invert the land polygons into per-tile sea polygons, then split any
    # multi-part tiles so every feature is a single Polygon.
    sea_gdf = land_to_sea_tiles(land_gdf, zoom)
    sea_gdf = sea_gdf.explode(ignore_index=True)
    sea_gdf = set_derived_identity(sea_gdf, zoom, source_created_at, produced_at)

    sea_gdf = gpd.GeoDataFrame(
        sea_gdf.reindex(columns=OUTPUT_COLUMNS),
        geometry="geometry",
        crs=land_gdf.crs,
    )

    write_parquet(sea_gdf, output_path)


@dataclass
class SeaPolygonArgs:
    coastline_path: Path
    output_path: Path
    zoom: int


def parse_args() -> SeaPolygonArgs:
    parser = argparse.ArgumentParser(
        description="Build the sea (moana) polygons for the water layer by inverting the land polygons"
    )
    parser.add_argument(
        "--coastline",
        type=str,
        dest="coastline_path",
        required=True,
        help="Path to land polygon parquet (coastline_polygon output)",
    )
    parser.add_argument("--output", type=str, dest="output_path", required=True, help="Path to output parquet")
    parser.add_argument(
        "--zoom",
        type=int,
        dest="zoom",
        default=SLICE_ZOOM,
        help=f"Web Mercator zoom level for slicing (default {SLICE_ZOOM})",
    )

    parsed = parser.parse_args()

    # Validate input parquet file
    if not os.path.isfile(parsed.coastline_path):
        sys.stderr.write(f"Error: coastline parquet file does not exist: {parsed.coastline_path}\n")
        sys.exit(1)

    # Ensure output directory exists
    output_path = Path(parsed.output_path)
    if not os.path.isdir(output_path.parent):
        try:
            os.makedirs(output_path.parent, exist_ok=True)
        except Exception as e:
            sys.stderr.write(f"Error: Output directory could not be created: {output_path.parent}. Details: {e}\n")
            sys.exit(1)

    return SeaPolygonArgs(
        coastline_path=Path(parsed.coastline_path),
        output_path=output_path,
        zoom=parsed.zoom,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    args = parse_args()

    run(args.coastline_path, args.output_path, args.zoom)
