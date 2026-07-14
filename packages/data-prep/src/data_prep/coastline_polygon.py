"""
Build the coastlines and islands polygon layer from source coastline lines and island polygons.
"""

import argparse
import logging
import uuid
from datetime import date
from pathlib import Path

import geopandas as gpd
import pandas as pd
import shapely
from osgeo import ogr

from data_prep.parquet_utils import write_parquet

ogr.UseExceptions()

logger = logging.getLogger(__name__)

NZTM2000 = 2193
NZGD2000 = 4167

# Round the coastline precision
PRECISION_TOLERANCE = 0.1

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
    "North Island": (1_800_000, 5_815_000),
    "South Island": (1_500_000, 5_180_000),
    "Stewart Island": (1_215_000, 4_782_000),
}


def read_and_project(path: Path, **read_kwargs) -> gpd.GeoDataFrame:
    gdf = gpd.read_parquet(path, **read_kwargs)
    epsg = gdf.crs.to_epsg() if gdf.crs else None
    if epsg != NZGD2000:
        raise ValueError(f"{path} must be NZGD2000 (EPSG:{NZGD2000}), got EPSG:{epsg}")
    return gdf.to_crs(NZTM2000)


def try_ogr_build_polygon(lines: list[ogr.Geometry], tolerance: float) -> ogr.Geometry | None:
    """
    Try OGR's BuildPolygonFromEdges — it handles snapping natively.
    Returns a geometry or None on failure.
    """
    # Collect all lines into a GeometryCollection
    gc = ogr.Geometry(ogr.wkbGeometryCollection)
    for l in lines:
        gc.AddGeometry(l)
    try:
        result = ogr.BuildPolygonFromEdges(gc, bBestEffort=True, bAutoClose=True, dfTolerance=tolerance)
        if result is not None and not result.IsEmpty():
            result.FlattenTo2D()
            return result
    except Exception as e:
        print(f"  BuildPolygonFromEdges failed: {e}")
        raise e
    return None


def coastline_to_polygons(coastline_gdf: gpd.GeoDataFrame, precision_tol: float) -> gpd.GeoSeries:
    """Convert coastline lines into land polygons using OGR BuildPolygonFromEdges."""
    ogr_lines = [ogr.CreateGeometryFromWkb(geom.wkb) for geom in coastline_gdf.geometry.values]

    ogr_polygon = try_ogr_build_polygon(ogr_lines, precision_tol)
    if ogr_polygon is None:
        raise ValueError("Coastline did not form any closed polygons; check for gaps in source linework.")

    polygon = shapely.make_valid(shapely.from_wkb(bytes(ogr_polygon.ExportToWkb())))
    polygons = shapely.get_parts(polygon)

    if len(polygons) == 0:
        raise ValueError("Coastline did not form any closed polygons; check for gaps in source linework.")

    return gpd.GeoSeries(polygons, crs=coastline_gdf.crs)


def set_derived_identity(land_gdf: gpd.GeoDataFrame, produced_at: date) -> gpd.GeoDataFrame:
    """Assign a uuid for id and produce-time timestamps to combined polygons."""
    result = land_gdf.copy()
    result["id"] = [str(uuid.uuid4()) for _ in range(len(result))]
    result["t50_fid"] = None
    result["created_at"] = produced_at
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

    # Convert the coastline lines into land polygons
    land_gdf = gpd.GeoDataFrame(geometry=coastline_to_polygons(coastline_gdf, PRECISION_TOLERANCE))
    land_named_gdf = name_land_polygons(land_gdf)
    # Land polygons are derived from the coastline lines, so tag them as such.
    land_named_gdf["type"] = "coastline"
    # Give each land polygon a reproducible id and produce-time timestamps.
    land_named_gdf = set_derived_identity(land_named_gdf, date.today())

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
