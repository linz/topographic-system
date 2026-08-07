import argparse
import logging
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import numpy
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


def run(input_path: Path, contour_path: Path, output_path: Path) -> None:
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

    write_parquet(output_gdf, output_path)


@dataclass
class CartoSymbolArgs:
    input_path: Path
    contour_path: Path
    output_path: Path


def parse_args() -> CartoSymbolArgs:
    parser = argparse.ArgumentParser(description="nztopo50_carto_symbols")
    parser.add_argument("--input", type=str, dest="input_path", required=True, help="Path to input gpkg")
    parser.add_argument("--contour", type=str, dest="contour_path", required=True, help="Path to contour parquet")
    parser.add_argument("--output", type=str, dest="output_path", required=True, help="Path to output parquet")

    parsed = parser.parse_args()

    return CartoSymbolArgs(
        input_path=Path(parsed.input_path), contour_path=Path(parsed.contour_path), output_path=Path(parsed.output_path)
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    args = parse_args()

    run(args.input_path, args.contour_path, args.output_path)
