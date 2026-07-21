"""Intersect Topo50 contour lines with ice landcover polygons.

Contour geometries are split at ice polygon boundaries. Each resulting
segment is tagged with the landcover type(ice) it falls within.
Processing is parallelised across available CPU cores.
"""

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from multiprocessing import Pool, cpu_count
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq

from data_prep.parquet_utils import write_parquet

logger = logging.getLogger(__name__)

# All inputs and the output are in NZGD2000
NZGD2000 = 4167

# Module-level globals so forked workers inherit via copy-on-write
# instead of pickling gigabytes of geodata per worker
_landcover_gdf = None
_contour_chunks = None


def process_chunk(chunk_idx):
    contour_chunk = _contour_chunks[chunk_idx]

    # Clip landcover to only geometries that intersect
    bounds = contour_chunk.total_bounds  # (minx, miny, maxx, maxy)
    landcover_subset = _landcover_gdf.cx[bounds[0] : bounds[2], bounds[1] : bounds[3]]

    if landcover_subset.empty:
        return None

    logger.info("start overlay chunk %d", chunk_idx)
    overlay_gdf = gpd.overlay(
        contour_chunk,
        landcover_subset,
        how="intersection",
        keep_geom_type=True,
    )
    logger.info("end overlay chunk %d", chunk_idx)

    return overlay_gdf


def split_gdf(gdf, n_chunks):
    if n_chunks <= 1:
        return [gdf]

    chunk_size = max(1, len(gdf) // n_chunks)
    return [gdf.iloc[i : i + chunk_size] for i in range(0, len(gdf), chunk_size)]


def read_nzgd2000(path: Path, **read_kwargs) -> gpd.GeoDataFrame:
    gdf = gpd.read_parquet(path, **read_kwargs)
    epsg = gdf.crs.to_epsg() if gdf.crs else None
    if epsg != NZGD2000:
        raise ValueError(f"{path} must be NZGD2000 (EPSG:{NZGD2000}), got EPSG:{epsg}")
    return gdf


def run(contour_path: Path, landcover_path: Path, overlay_path: Path) -> None:
    global _landcover_gdf, _contour_chunks

    contour_gdf = read_nzgd2000(contour_path).drop(columns=["updated_at", "version"])

    landcover_geom_col = json.loads(pq.read_schema(landcover_path).metadata[b"geo"])["primary_column"]
    _landcover_gdf = read_nzgd2000(
        landcover_path,
        filters=[("type", "==", "ice")],
        columns=[
            "id",
            "type",
            "updated_at",
            "version",
            landcover_geom_col,
        ],
    ).rename(columns={"id": "landcover_id", "type": "landcover_type"})

    n_workers = cpu_count()
    n_chunks = 100

    _contour_chunks = split_gdf(contour_gdf, n_chunks)
    del contour_gdf

    with Pool(n_workers, maxtasksperchild=1) as pool:
        results = [
            r for r in pool.imap_unordered(process_chunk, range(len(_contour_chunks))) if r is not None and not r.empty
        ]

    _landcover_gdf = None
    _contour_chunks = None

    overlay_gdf = gpd.GeoDataFrame(pd.concat(results, ignore_index=True))
    overlay_gdf = overlay_gdf.set_crs(epsg=NZGD2000)

    write_parquet(overlay_gdf, overlay_path)


@dataclass
class IceContourArgs:
    contour_path: Path
    landcover_path: Path
    output_path: Path


def parse_args() -> IceContourArgs:
    parser = argparse.ArgumentParser(description="Overlay contour with landcover to produce ice contour")
    parser.add_argument("--contour", type=str, dest="contour_path", required=True, help="Path to contour parquet")
    parser.add_argument("--landcover", type=str, dest="landcover_path", required=True, help="Path to landcover parquet")
    parser.add_argument("--output", type=str, dest="output_path", required=True, help="Path to output parquet")

    parsed = parser.parse_args()

    # Validate input parquet files
    for label, path in (("contour", parsed.contour_path), ("landcover", parsed.landcover_path)):
        if not os.path.isfile(path):
            sys.stderr.write(f"Error: {label} parquet file does not exist: {path}\n")
            sys.exit(1)

    # Ensure output directory exists
    output_path = Path(parsed.output_path)
    if not os.path.isdir(output_path.parent):
        try:
            os.makedirs(output_path.parent, exist_ok=True)
        except Exception as e:
            sys.stderr.write(f"Error: Output directory could not be created: {output_path.parent}. Details: {e}\n")
            sys.exit(1)

    return IceContourArgs(
        contour_path=Path(parsed.contour_path),
        landcover_path=Path(parsed.landcover_path),
        output_path=output_path,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    args = parse_args()

    run(args.contour_path, args.landcover_path, args.output_path)
