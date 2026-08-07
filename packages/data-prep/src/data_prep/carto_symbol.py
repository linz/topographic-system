import argparse
import logging
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import numpy
import pandas as pd
import shapely

from data_prep.parquet_utils import read_and_project, write_parquet

logger = logging.getLogger(__name__)


def chop(gdf: gpd.GeoDataFrame, stride: int = 64) -> gpd.GeoDataFrame:
    """Split lines into pieces of at most `stride` segments."""
    gdf = gdf.explode(index_parts=False).reset_index(drop=True)

    owner = []
    pieces = []
    for index, geometry in enumerate(gdf.geometry.to_numpy()):
        coords = shapely.get_coordinates(geometry)
        for start in range(0, len(coords) - 1, stride):
            owner.append(index)
            pieces.append(shapely.LineString(coords[start : start + stride + 1]))

    attrs = gdf.drop(columns=gdf.geometry.name).iloc[owner].reset_index(drop=True)
    return gpd.GeoDataFrame(attrs, geometry=pieces, crs=gdf.crs)


def add_contour_numbers(input_path: Path, contour_path: Path) -> gpd.GeoDataFrame:
    contour_number_gdf = gpd.read_file(input_path, layer="contour_number")
    contour_number_gdf["orientation"] = (
        numpy.round(numpy.degrees(contour_number_gdf["orientation"].astype("float64"))).astype("int32") % 360
    )

    contour_gdf = read_and_project(contour_path, columns=["topo_id", "elevation", "geometry"])
    contour_chop_gdf = chop(contour_gdf)

    output_gdf = gpd.sjoin_nearest(contour_number_gdf, contour_chop_gdf, how="left", max_distance=1)
    output_gdf = output_gdf.reset_index(names="point_index")
    output_gdf = output_gdf.sort_values("point_index").drop_duplicates("point_index", keep="first")
    output_gdf = output_gdf[["topo_id", "orientation", "elevation", output_gdf.geometry.name]].rename(
        columns={"topo_id": "source_id", "elevation": "label"}
    )
    output_gdf["label"] = output_gdf["label"].astype("Int64").astype("string")
    output_gdf["type"] = "contour_number"

    return output_gdf


def add_highway_shields(input_path: Path, road_line_path: Path) -> gpd.GeoDataFrame:
    highway_sh_gdf = gpd.read_file(input_path, layer="linz_highway_sh")

    road_line_gdf = read_and_project(road_line_path, columns=["id", "highway_number", "geometry"])

    output_gdf = gpd.sjoin_nearest(highway_sh_gdf, road_line_gdf, how="left", max_distance=1)
    output_gdf = output_gdf[["id", "highway_number", output_gdf.geometry.name]].rename(
        columns={"id": "source_id", "highway_number": "label"}
    )
    output_gdf["type"] = "highway_shield"

    return output_gdf


def add_golf_courses(input_path: Path, landuse_path: Path) -> gpd.GeoDataFrame:
    golf_sym_gdf = gpd.read_file(input_path, layer="golf_sym")

    land_use_gdf = read_and_project(landuse_path, columns=["id", "geometry"])

    output_gdf = gpd.sjoin(golf_sym_gdf, land_use_gdf, predicate="within")
    output_gdf = output_gdf[["id", output_gdf.geometry.name]].rename(columns={"id": "source_id"})
    output_gdf["type"] = "golf_course"

    return output_gdf


def add_mines(input_path: Path, landuse_path: Path) -> gpd.GeoDataFrame:
    mine_sym_gdf = gpd.read_file(input_path, layer="mine_sym")

    land_use_gdf = read_and_project(landuse_path, columns=["id", "geometry"])

    output_gdf = gpd.sjoin(mine_sym_gdf, land_use_gdf, predicate="within")
    output_gdf = output_gdf[["id", "mine_vis", output_gdf.geometry.name]].rename(
        columns={"id": "source_id", "mine_vis": "type"}
    )

    return output_gdf


def run(input_path: Path, contour_path: Path, road_line_path: Path, landuse_path: Path, output_path: Path) -> None:
    contour_number_gdf = add_contour_numbers(input_path, contour_path)
    golf_sym_gdf = add_golf_courses(input_path, landuse_path)
    mine_sym_gdf = add_mines(input_path, landuse_path)
    highway_sh_gdf = add_highway_shields(input_path, road_line_path)
    output_gdf = gpd.GeoDataFrame(
        pd.concat([contour_number_gdf, golf_sym_gdf, mine_sym_gdf, highway_sh_gdf], ignore_index=True)
    )
    write_parquet(output_gdf, output_path)


@dataclass
class CartoSymbolArgs:
    input_path: Path
    contour_path: Path
    road_line_path: Path
    landuse_path: Path
    output_path: Path


def parse_args() -> CartoSymbolArgs:
    parser = argparse.ArgumentParser(description="nztopo50_carto_symbols")
    parser.add_argument("--input", type=str, dest="input_path", required=True, help="Path to input gpkg")
    parser.add_argument("--contour", type=str, dest="contour_path", required=True, help="Path to contour parquet")
    parser.add_argument("--road", type=str, dest="road_line_path", required=True, help="Path to road_line parquet")
    parser.add_argument("--landuse", type=str, dest="landuse_path", required=True, help="Path to landuse parquet")
    parser.add_argument("--output", type=str, dest="output_path", required=True, help="Path to output parquet")

    parsed = parser.parse_args()

    return CartoSymbolArgs(
        input_path=Path(parsed.input_path),
        contour_path=Path(parsed.contour_path),
        road_line_path=Path(parsed.road_line_path),
        landuse_path=Path(parsed.landuse_path),
        output_path=Path(parsed.output_path),
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run(args.input_path, args.contour_path, args.road_line_path, args.landuse_path, args.output_path)
