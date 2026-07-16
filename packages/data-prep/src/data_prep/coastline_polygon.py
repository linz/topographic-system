"""
Build the coastlines and islands polygon layer from source coastline lines and island polygons.
"""

import argparse
import logging
from datetime import UTC, date, datetime, time
from pathlib import Path

import geopandas as gpd
import pandas as pd
import shapely
from kart_import.uuid7 import reproducable_uuid7_text

from data_prep.parquet_utils import write_parquet

logger = logging.getLogger(__name__)

NZTM2000 = 2193
NZGD2000 = 4167

# Round the coastline precision
PRECISON_TOLERANCE = 0.1

# Output properties for the merged coastline and island polygons.
OUTPUT_COLUMNS = [
    "id",
    "t50_fid",
    "type",
    "coastline_type",
    "elevation",
    "name",
    "group_name",
    "updated_at",
    "created_at",
    "geometry",
]

# Interior reference points (NZTM2000, EPSG:2193) used to identify the named
NAME_REFERENCE_POINTS = {
    "Te Ika-a-Māui or North Island": (1_800_000, 5_815_000),
    "Te Waipounamu or South Island": (1_500_000, 5_180_000),
    "Stewart Island": (1_215_000, 4_782_000),
}


def read_and_project(path: Path, **read_kwargs) -> gpd.GeoDataFrame:
    gdf = gpd.read_parquet(path, **read_kwargs)
    epsg = gdf.crs.to_epsg() if gdf.crs else None
    if epsg != NZGD2000:
        raise ValueError(f"{path} must be NZGD2000 (EPSG:{NZGD2000}), got EPSG:{epsg}")
    return gdf.to_crs(NZTM2000)


def coastline_to_polygons(coastline_gdf: gpd.GeoDataFrame, tolerance: float) -> gpd.GeoSeries:
    """Convert coastline lines into land polygons."""
    geoms = coastline_gdf.geometry
    if tolerance > 0:
        geoms = geoms.apply(lambda g: shapely.set_precision(g, grid_size=tolerance))

    noded = shapely.unary_union(geoms.values)

    edges = shapely.get_parts(noded)
    polygons = shapely.get_parts(shapely.polygonize(edges))

    if len(polygons) == 0:
        raise ValueError("Coastline did not form any closed polygons; check for gaps in source linework.")

    return gpd.GeoSeries(polygons, crs=coastline_gdf.crs)


def earliest_created_at(gdf: gpd.GeoDataFrame) -> date:
    """Return the earliest ``created_at`` date in the source."""
    if "created_at" not in gdf.columns:
        raise ValueError("Source has no created_at column; cannot derive a stable created_at.")
    created_at = pd.to_datetime(gdf["created_at"], errors="coerce").dropna()
    if created_at.empty:
        raise ValueError("Source has no valid created_at values; cannot derive a stable created_at.")
    return created_at.min().date()


def set_derived_identity(land_gdf: gpd.GeoDataFrame, source_created_at: date, produced_at: date) -> gpd.GeoDataFrame:
    """Assign a reproducible uuid for id and timestamps to combined polygons.

    The UUIDv7 timestamp and ``created_at`` come from the earliest source
    ``created_at`` so ids stay stable across reruns; ``updated_at`` is the produce time.
    """
    result = land_gdf.copy()
    if result["name"].isna().any():
        raise ValueError("Cannot derive a reproducible id: one or more land polygons have no name.")
    timestamp_ms = int(datetime.combine(source_created_at, time.min, tzinfo=UTC).timestamp() * 1000)
    # Derive a reproducible UUIDv7 from the source timestamp and the name.
    result["id"] = [str(reproducable_uuid7_text(timestamp_ms, name)) for name in result["name"]]
    result["t50_fid"] = None
    result["created_at"] = source_created_at
    result["updated_at"] = produced_at
    return result


def name_land_polygons(land_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Name each land polygon by the interior reference point it contains."""
    named = gpd.GeoDataFrame(
        {"name": pd.Series([None] * len(land_gdf), dtype="object")},
        geometry=land_gdf.geometry,
        crs=land_gdf.crs,
    )

    for name, (x, y) in NAME_REFERENCE_POINTS.items():
        contains_point = named.geometry.contains(shapely.Point(x, y))
        if contains_point.any():
            named.loc[contains_point, "name"] = name
        else:
            logger.warning("no land polygon contains the reference point for %s", name)

    return named


def run(coastline_path: Path, island_path: Path, output_path: Path) -> None:
    coastline_gdf = read_and_project(coastline_path)
    island_gdf = read_and_project(island_path)

    produced_at = date.today()
    # Use the earliest source created_at so derived ids stay stable across reruns.
    source_created_at = earliest_created_at(coastline_gdf)

    # Convert the coastline lines into land polygons
    land_gdf = gpd.GeoDataFrame(geometry=coastline_to_polygons(coastline_gdf, PRECISON_TOLERANCE))
    land_named_gdf = name_land_polygons(land_gdf)
    # Land polygons are derived from the coastline lines, so tag them as such.
    land_named_gdf["type"] = "coastline"
    # Give each land polygon a reproducible id and timestamps.
    land_named_gdf = set_derived_identity(land_named_gdf, source_created_at, produced_at)

    # Exclude the island polygons that fall within the coastline polygons.
    land_union = land_named_gdf.geometry.union_all()
    island_gdf = island_gdf[~island_gdf.geometry.within(land_union)]

    # Merge coastline and island polygons
    merged = pd.concat([land_named_gdf, island_gdf], ignore_index=True)
    coastlines_islands_gdf = gpd.GeoDataFrame(
        merged.reindex(columns=OUTPUT_COLUMNS),
        geometry="geometry",
        crs=island_gdf.crs,
    ).to_crs(NZGD2000)

    write_parquet(coastlines_islands_gdf, output_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(
        description="Build the coastlines and islands polygon layer from coastline lines and island polygons"
    )
    parser.add_argument("--coastline", required=True, help="Path to coastline parquet (lines)")
    parser.add_argument("--island", required=True, help="Path to island parquet (polygons)")
    parser.add_argument("--output", required=True, help="Path to output parquet")
    args = parser.parse_args()

    run(Path(args.coastline), Path(args.island), Path(args.output))
