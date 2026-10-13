"""Restore integer columns that kart declares but an export cannot carry.

OGR has no decimal type, so `kart export` writes a kart `numeric` as REAL: `linz_road_cl.t50_fid`
is `numeric(10, 0)` in the repo but float64 on read. A nullable OGR integer widens to float64 too,
having nowhere to put the NULL. No format `kart export` offers avoids the first, but the
declaration is still in the repo, so take the type from there.

Restoring at load keeps the float out of the pipeline, rather than leaving the joins to meet an
int key on one side and a float on the other.
"""

import logging
from collections.abc import Iterable
from typing import Final

import pandas as pd

logger = logging.getLogger("kart_import")

INT_DTYPE: Final = "Int64"
"""What a declared integer is restored to. Nullable: the float being cast back may carry NULLs."""

_FLOAT_EXACT_INT_LIMIT = 2**53
"""Above this a float64 cannot distinguish adjacent integers, so the value may already be the
wrong one - not something a cast can undo."""


def integer_columns(schema: Iterable[dict]) -> set[str]:
    """The names in `schema` declared as whole numbers.

    A bare DECIMAL(p) is DECIMAL(p, 0) in SQL, so scale may be absent; a DECIMAL with neither
    precision nor scale is unconstrained and may be fractional. `precision` alone decides nothing
    - it is a ceiling, not an observed range (`lol_sufi` is `numeric(20, 0)` but stops around
    3.1e6), so `coerce_integer_columns` asks the values instead.
    """
    names = set()
    for field in schema:
        scale = field.get("scale")
        if field.get("dataType") == "integer" or (
            field.get("dataType") == "numeric" and (scale == 0 or (scale is None and "precision" in field))
        ):
            names.add(field["name"])
    return names


def coerce_integer_columns[DF: pd.DataFrame](df: DF, schema: Iterable[dict], *, context: str) -> DF:
    """`df` with every float column `schema` declares a whole number cast to `INT_DTYPE`.

    Only float columns are touched. Raises where the values contradict the declaration: a
    fractional value, or one past `_FLOAT_EXACT_INT_LIMIT`. Every declared column is checked, not
    just join keys, so either fails the whole load.
    """
    declared = integer_columns(schema)
    targets = [col for col in df.columns if col in declared and pd.api.types.is_float_dtype(df[col].dtype)]
    if not targets:
        return df

    df = df.copy()
    for col in targets:
        # Only the limit needs checking here: `astype` rejects a fractional value itself, but
        # accepts one already rounded past float64's exact-integer range.
        if bool((df[col].abs() >= _FLOAT_EXACT_INT_LIMIT).any()):
            raise ValueError(
                f"{context}: column {col!r} is declared a whole number but holds values at or "
                f"beyond {_FLOAT_EXACT_INT_LIMIT} (float64's exact-integer limit), where the "
                f"exported float may already have rounded to a different integer"
            )
        try:
            df[col] = df[col].astype(INT_DTYPE)
        except TypeError as e:
            raise ValueError(
                f"{context}: column {col!r} is declared a whole number but the export holds "
                f"fractional values; the export and the kart schema disagree"
            ) from e

    logger.info(
        "restored declared integer columns",
        extra={"context": context, "columns": targets, "to": INT_DTYPE},
    )
    return df
