"""Prepare step: build a slim lookup table from a source dataset's export.

A `Lookup` selects/renames a few columns from its source, dropping geometry and rows without a key, deduped on the key.
The result is a small attribute table that the transform stage left-joins into emitted datasets.
Lookups are never emitted as theme features.
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


def prepare_lookup(lookup_name: str) -> Path:
    """Slim each of the lookup's per-commit exports into a parquet (keyed by commit)."""
    lookup = get_lookup_by_name(lookup_name)

    input_dir = WORKING_EXPORTS_DIR / "lookup" / lookup_name
    if not input_dir.exists():
        raise FileNotFoundError(f"no exported lookup {lookup_name!r} at {input_dir}")

    output_dir = WORKING_LOOKUP_DIR / lookup_name
    output_dir.mkdir(parents=True, exist_ok=True)

    for input_file in sorted(input_dir.glob("*.json")):
        commit = input_file.stem
        output_file = output_dir / f"{commit}.parquet"

        start_time = time.perf_counter()
        gdf = gpd.read_file(input_file, engine="pyogrio", use_arrow=True)
        out = select_lookup_columns(gdf, lookup)
        out.to_parquet(output_file, compression="zstd", index=False)
        logger.info(
            "prepare_lookup",
            extra={
                "lookup": lookup_name,
                "commit": commit,
                "rows": len(out),
                "duration": round(time.perf_counter() - start_time, 4),
            },
        )
    return output_dir


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m kart_import.assets.prepare <lookup_name>")
        sys.exit(1)
    with log_context(action="prepare", lookup=sys.argv[1]):
        prepare_lookup(sys.argv[1])
