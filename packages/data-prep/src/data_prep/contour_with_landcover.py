import geopandas as gpd
import pandas as pd
import argparse

from pathlib import Path
from multiprocessing import cpu_count, get_context
from data_prep.parquet_utils import write_parquet


def process_chunk(args):
    contour_chunk, landcover_gdf = args

    overlay_gdf = gpd.overlay(
        contour_chunk,
        landcover_gdf,
        how="union",
        keep_geom_type=True,
    )

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
    contour_gdf = gpd.read_parquet(contour_path)
    landcover_gdf = gpd.read_parquet(landcover_path)

    # Don't use more workers than rows
    n_workers = min(cpu_count(), len(contour_gdf))

    contour_chunks = split_gdf(contour_gdf, n_workers)

    # Use spawn to avoid fork-related issues with GeoPandas/Shapely
    with get_context("spawn").Pool(n_workers) as pool:
        results = pool.map(
            process_chunk,
            [(chunk, landcover_gdf) for chunk in contour_chunks],
        )

    results = [r for r in results if r is not None and not r.empty]

    overlay_gdf = gpd.GeoDataFrame(pd.concat(results, ignore_index=True))

    write_parquet(overlay_gdf, overlay_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Overlay contour with landcover")
    parser.add_argument("--contour", required=True, help="Path to contour parquet")
    parser.add_argument("--landcover", required=True, help="Path to landcover parquet")
    parser.add_argument("--output", required=True, help="Path to output directory")
    args = parser.parse_args()

    run(Path(args.contour), Path(args.landcover), Path(args.output))
