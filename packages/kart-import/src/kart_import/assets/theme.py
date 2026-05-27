import logging

import geopandas as gpd
import pandas as pd

from ..config import WORKING_THEME_DIR, WORKING_TRANSFORM_DIR, get_theme_by_name
from ..log import log_context

logger = logging.getLogger("kart_import")


def merge_theme_release(theme_name: str, release_id: int):
    theme = get_theme_by_name(theme_name)

    logger.info(f"Merging theme: {theme.name} for release {release_id}")

    release_dir = WORKING_THEME_DIR / f"release_{release_id}"
    release_dir.mkdir(parents=True, exist_ok=True)
    output_geojson = release_dir / f"{theme.name}.geojson"
    gdfs = []

    if output_geojson.exists():
        output_geojson.unlink()

    has_missing = False
    for dataset in theme.datasets:
        geojson_path = WORKING_TRANSFORM_DIR / f"release_{release_id}" / f"{dataset.name}.json"
        if not geojson_path.exists():
            logger.warning(
                f"Transformed file not found: {geojson_path}. Skipping.", extra={"source_dataset": dataset.name}
            )
            has_missing = True
            continue
        gdf = gpd.read_file(geojson_path, engine="pyogrio", use_arrow=True)
        if gdf.empty:
            logger.info(f"{dataset.name} (release {release_id}) is empty. Skipping.")
            continue

        logger.info(f"{dataset.name} (release {release_id}): {len(gdf)} features")
        gdfs.append(gdf)

    if has_missing:
        raise Exception("Missing source datasets run transform")

    if not gdfs:
        logger.warning(f"No data found for theme {theme.name} release {release_id}.")
        return

    merged = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)

    # Stable sorting to keep row order predictable
    if "id" in merged.columns:
        merged = merged.sort_values(by=["id"]).reset_index(drop=True)

    logger.info(f"Writing {len(merged)} total features → {output_geojson}")

    # Explicitly remove fid if it exists and ensure index is not written
    if "fid" in merged.columns:
        merged = merged.drop(columns=["fid"])

    merged.to_file(output_geojson, driver="GeoJSON", index=False)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python -m kart_import.assets.theme <theme_name> <release_id>")
        sys.exit(1)
    with log_context(action="theme", theme=sys.argv[1], release=int(sys.argv[2])):
        merge_theme_release(sys.argv[1], int(sys.argv[2]))
