"""
Build the sea (moana) polygons for the water layer from the derived land polygons.

The land polygons (produced by ``coastline_polygon.py``) are inverted to produce
the sea. To avoid a single large polygon, the sea is sliced by Web Mercator
quadkey tiles at a fixed zoom level: each tile contributes one ``type = moana``
water feature clipped to that tile's extent.
"""

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import geopandas as gpd
import pandas as pd
import shapely

from data_prep.identity import earliest_created_at, reproducible_uuid7
from data_prep.parquet_utils import NZGD2000, read_and_project, write_parquet

logger = logging.getLogger(__name__)

WEB_MERCATOR = 3857

# Web Mercator zoom level used to slice the sea into tile-sized extents.
SLICE_ZOOM = 7

# Half the Web Mercator world extent in metres (EPSG:3857 spans +/- this value).
WEB_MERCATOR_HALF = 20037508.3427892

# Output properties for the sea polygons written into the water layer.
OUTPUT_COLUMNS = [
    "id",
    "quadkey",
    "type",
    "updated_at",
    "created_at",
    "geometry",
]


def tile_bounds(x: int, y: int, zoom: int) -> tuple[float, float, float, float]:
    """Return the (minx, miny, maxx, maxy) Web Mercator bounds of a tile.

    Tile ``x`` increases eastward from the western edge; tile ``y`` increases
    southward from the northern edge (slippy-map / quadkey convention).
    """
    tile_size = 2 * WEB_MERCATOR_HALF / (2**zoom)
    minx = -WEB_MERCATOR_HALF + x * tile_size
    maxy = WEB_MERCATOR_HALF - y * tile_size
    return minx, maxy - tile_size, minx + tile_size, maxy


def tile_range(bounds: tuple[float, float, float, float], zoom: int):
    """Yield the (x, y) tiles at ``zoom`` that cover a Web Mercator bounding box."""
    minx, miny, maxx, maxy = bounds
    n = 2**zoom
    tile_size = 2 * WEB_MERCATOR_HALF / n

    def clamp(value: int) -> int:
        return max(0, min(value, n - 1))

    x_start = clamp(int((minx + WEB_MERCATOR_HALF) // tile_size))
    x_end = clamp(int((maxx + WEB_MERCATOR_HALF) // tile_size))
    y_start = clamp(int((WEB_MERCATOR_HALF - maxy) // tile_size))
    y_end = clamp(int((WEB_MERCATOR_HALF - miny) // tile_size))

    for x in range(x_start, x_end + 1):
        for y in range(y_start, y_end + 1):
            yield x, y


def tile_quadkey(x: int, y: int, zoom: int) -> str:
    """Return the Web Mercator quadkey string for a tile."""
    digits = []
    for i in range(zoom, 0, -1):
        digit = 0
        mask = 1 << (i - 1)
        if x & mask:
            digit += 1
        if y & mask:
            digit += 2
        digits.append(str(digit))
    return "".join(digits)


def land_to_sea_tiles(land_gdf: gpd.GeoDataFrame, zoom: int) -> gpd.GeoDataFrame:
    """Invert the land polygons into per-tile sea polygons.

    Each Web Mercator tile overlapping the land extent yields one sea polygon
    (the tile with the land subtracted). Tiles fully covered by land are dropped.
    """
    web_gdf = land_gdf.to_crs(WEB_MERCATOR)
    land_union = web_gdf.geometry.union_all()

    xs: list[int] = []
    ys: list[int] = []
    geometries: list[shapely.Geometry] = []
    for x, y in tile_range(tuple(web_gdf.total_bounds), zoom):
        tile = shapely.box(*tile_bounds(x, y, zoom))
        sea = tile.difference(land_union) if tile.intersects(land_union) else tile
        if sea.is_empty:
            continue
        xs.append(x)
        ys.append(y)
        geometries.append(sea)

    sea_gdf = gpd.GeoDataFrame({"x": xs, "y": ys}, geometry=geometries, crs=WEB_MERCATOR)
    return sea_gdf.to_crs(land_gdf.crs)


def set_derived_identity(
    sea_gdf: gpd.GeoDataFrame, zoom: int, source_created_at: date, produced_at: date
) -> gpd.GeoDataFrame:
    """Assign the quadkey, a reproducible id and timestamps to each sea polygon.

    The quadkey is derived from each tile's ``x``/``y`` index. The UUIDv7
    timestamp and ``created_at`` come from the source land polygons' earliest
    ``created_at`` so ids stay stable across reruns; the quadkey makes each
    tile's id unique. ``updated_at`` is the produce time.
    """
    result = sea_gdf.copy()

    result["quadkey"] = [tile_quadkey(x, y, zoom) for x, y in zip(result["x"], result["y"], strict=True)]
    timestamp_ms = int(pd.Timestamp(source_created_at).timestamp() * 1000)
    result["id"] = [str(reproducible_uuid7(timestamp_ms, quadkey)) for quadkey in result["quadkey"]]
    result["type"] = "moana"
    result["created_at"] = source_created_at
    result["updated_at"] = produced_at
    return result


def run(coastline_path: Path, output_path: Path, zoom: int = SLICE_ZOOM) -> None:
    land_gdf = read_and_project(coastline_path, target_crs=NZGD2000)

    produced_at = date.today()
    # Use the earliest source created_at so derived ids stay stable across reruns.
    source_created_at = earliest_created_at(land_gdf)

    # Invert the land polygons into per-tile sea polygons.
    sea_gdf = land_to_sea_tiles(land_gdf, zoom)
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
