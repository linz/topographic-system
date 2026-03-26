import geopandas as gpd
import pandas as pd
import argparse

from pathlib import Path
from multiprocessing import cpu_count, Pool
from data_prep.parquet_utils import write_parquet
from datetime import datetime

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
        overlay_gdf = contour_chunk.copy()
        overlay_gdf["landcover_feature_type"] = "other"
        overlay_gdf["landcover_topo_id"] = pd.NA
        return overlay_gdf

    print("start overlay:", datetime.now())
    overlay_gdf = gpd.overlay(
        contour_chunk,
        landcover_subset,
        how="union",
        keep_geom_type=True,
    )
    print("end overlay:", datetime.now())

    overlay_gdf = overlay_gdf.rename(
        columns={
            "feature_type_1": "feature_type",
            "topo_id_1": "topo_id",
            "feature_type_2": "landcover_feature_type",
            "topo_id_2": "landcover_topo_id",
        }
    )

    overlay_gdf["landcover_feature_type"] = overlay_gdf[
        "landcover_feature_type"
    ].fillna("other")

    return overlay_gdf


def split_gdf(gdf, n_chunks):
    if n_chunks <= 1:
        return [gdf]

    chunk_size = max(1, len(gdf) // n_chunks)
    return [gdf.iloc[i : i + chunk_size] for i in range(0, len(gdf), chunk_size)]


def run(contour_path: Path, landcover_path: Path, overlay_path: Path) -> None:
    global _landcover_gdf, _contour_chunks

    contour_gdf = gpd.read_parquet(contour_path)
    _landcover_gdf = gpd.read_parquet(landcover_path)

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
    parser = argparse.ArgumentParser(description="Overlay contour with landcover")
    parser.add_argument("--contour", required=True, help="Path to contour parquet")
    parser.add_argument("--landcover", required=True, help="Path to landcover parquet")
    parser.add_argument("--output", required=True, help="Path to output directory")
    args = parser.parse_args()

    run(Path(args.contour), Path(args.landcover), Path(args.output))
