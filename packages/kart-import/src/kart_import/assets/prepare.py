"""Prepare step: build a slim lookup table from a source dataset's export.

A `Lookup` selects/renames a few columns from its source (keyed by `key`),
dropping geometry and rows without a key, deduped on the key. The result is a
small attribute table that the transform stage left-joins into emitted datasets
(see `transform.apply_joins`). Lookups are never emitted as theme features.
"""

import logging
import time
from pathlib import Path

import geopandas as gpd
import pandas as pd

from ..config import (
    WORKING_EXPORTS_DIR,
    WORKING_LOOKUP_DIR,
    Lookup,
    get_lookup_by_name,
)
from ..log import log_context

logger = logging.getLogger("kart_import")


def select_lookup_columns(gdf: gpd.GeoDataFrame, lookup: Lookup) -> pd.DataFrame:
    """The key + selected/renamed columns as a plain (geometry-free) DataFrame."""
    if lookup.key not in gdf.columns:
        raise KeyError(f"Lookup '{lookup.name}' key column '{lookup.key}' not found in source")

    out = pd.DataFrame({lookup.key: gdf[lookup.key].to_numpy()})
    for target, expr in lookup.columns.items():
        if not (isinstance(expr, str) and expr.startswith("$")):
            raise ValueError(f"Lookup '{lookup.name}' column '{target}' must reference a source column ($col)")
        source_col = target if expr == "$" else expr[1:]
        if source_col not in gdf.columns:
            raise KeyError(f"Lookup '{lookup.name}' source column '{source_col}' not found")
        out[target] = gdf[source_col].to_numpy()

    # A lookup must be unique on its key (left-joins assume one row per key).
    keyed = out[out[lookup.key].notna()]
    before = len(keyed)
    keyed = keyed.drop_duplicates(subset=[lookup.key], keep="first")
    if len(keyed) != before:
        logger.warning(f"Lookup '{lookup.name}' had {before - len(keyed)} duplicate '{lookup.key}' rows; kept first")
    return keyed.reset_index(drop=True)


def prepare_lookup_release(lookup_name: str, release_id: int) -> Path:
    lookup = get_lookup_by_name(lookup_name)

    input_file = WORKING_EXPORTS_DIR / f"release_{release_id}" / f"{lookup_name}.json"
    if not input_file.exists():
        raise FileNotFoundError(input_file)

    output_dir = WORKING_LOOKUP_DIR / f"release_{release_id}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{lookup_name}.parquet"

    start_time = time.perf_counter()
    gdf = gpd.read_file(input_file, engine="pyogrio", use_arrow=True)
    out = select_lookup_columns(gdf, lookup)
    out.to_parquet(output_file, compression="zstd", index=False)
    logger.info(
        "prepare_lookup",
        extra={
            "lookup": lookup_name,
            "release": release_id,
            "rows": len(out),
            "duration": round(time.perf_counter() - start_time, 4),
        },
    )
    return output_file


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python -m kart_import.assets.prepare <lookup_name> <release_id>")
        sys.exit(1)
    with log_context(action="prepare", lookup=sys.argv[1], release=sys.argv[2]):
        prepare_lookup_release(sys.argv[1], int(sys.argv[2]))
