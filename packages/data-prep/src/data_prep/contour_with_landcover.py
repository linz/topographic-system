"""Intersect Topo50 contour lines with ice landcover polygons.

Contour geometries are split at ice polygon boundaries. Each resulting
segment is tagged with the landcover feature type(ice) it falls within.
Processing is parallelised across available CPU cores.

Output schema: contour_with_landcover.yaml
"""

import argparse
import json
import logging
from multiprocessing import Pool, cpu_count
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq

from data_prep.parquet_utils import write_parquet

logger = logging.getLogger(__name__)

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


def run(contour_path: Path, landcover_path: Path, overlay_path: Path) -> None:
    global _landcover_gdf, _contour_chunks

    # TODO remove once we use geometry column everywhere
    # work out what the geometry column is called(geom or geometry?) by looking at metadata
    contour_geom_col = json.loads(pq.read_schema(contour_path).metadata[b"geo"])[
        "primary_column"
    ]
    contour_gdf = gpd.read_parquet(
        contour_path,
        columns=["topo_id", "feature_type", contour_geom_col],
    )

    landcover_geom_col = json.loads(pq.read_schema(landcover_path).metadata[b"geo"])[
        "primary_column"
    ]
    _landcover_gdf = gpd.read_parquet(
        landcover_path,
        filters=[("feature_type", "==", "ice")],
        columns=[
            "topo_id",
            "feature_type",
            "update_date",
            "version",
            landcover_geom_col,
        ],
    ).rename(
        columns={"topo_id": "landcover_id", "feature_type": "landcover_feature_type"}
    )

    n_workers = cpu_count()
    n_chunks = 100

    _contour_chunks = split_gdf(contour_gdf, n_chunks)
    del contour_gdf

    with Pool(n_workers, maxtasksperchild=1) as pool:
        results = [
            r
            for r in pool.imap_unordered(process_chunk, range(len(_contour_chunks)))
            if r is not None and not r.empty
        ]

    _landcover_gdf = None
    _contour_chunks = None

    overlay_gdf = gpd.GeoDataFrame(pd.concat(results, ignore_index=True))

    write_parquet(overlay_gdf, overlay_path)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    parser = argparse.ArgumentParser(description="Overlay contour with landcover")
    parser.add_argument("--contour", required=True, help="Path to contour parquet")
    parser.add_argument("--landcover", required=True, help="Path to landcover parquet")
    parser.add_argument("--output", required=True, help="Path to output parquet")
    args = parser.parse_args()

    run(Path(args.contour), Path(args.landcover), Path(args.output))
