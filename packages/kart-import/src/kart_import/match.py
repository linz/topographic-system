"""Type-tolerant value matching for config-driven transforms.

Config values come from YAML (strings or ints), but pyogrio may read a column as int,
float, or string depending on the data - an integer id can arrive as ``5.0`` when nulls
widen the column to float. `normalise` canonicalises a value, or a whole Series, so that
``5``, ``5.0`` and ``"5"`` compare equal, while genuine strings are left untouched so a
leading-zero id like ``"007"`` is never collapsed to ``7``. NaN/None canonicalise to
``None`` and therefore never match a real key.

This is the single home for the int/float/string matching rule shared by `corrections`,
`fixups`, and lookup joins - prefer it over ad-hoc ``astype(str)`` or ``to_numeric``.
"""

from __future__ import annotations

from typing import overload

import pandas as pd


@overload
def normalise(value: pd.Series) -> pd.Series: ...
@overload
def normalise(value: object) -> object: ...
def normalise(value: object) -> object:
    """Canonical comparable form of a scalar, or an object-dtype Series of them.

    Pass a pandas Series to canonicalise a whole column; pass any scalar (e.g. a value
    read from a YAML config) to canonicalise one value. A scalar and a column cell that
    represent the same number always normalise to the same string, so a mask such as
    ``normalise(series) == normalise(config_value)`` is type-tolerant by construction.
    """
    if isinstance(value, pd.Series):
        return _normalise_series(value)
    # Route the scalar through the same Series logic so the two paths can never diverge.
    return _normalise_series(pd.Series([value])).iloc[0]


def _normalise_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        # Bools stringify like the scalar path ("True"/"False"); rare as a key.
        canon = series.astype("string")
    elif pd.api.types.is_numeric_dtype(series):
        num = pd.to_numeric(series, errors="coerce")
        # Render whole numbers without a trailing ".0" so 5.0 matches 5 and "5";
        # non-integral values keep their full string form (e.g. "2.5").
        integral = num.notna() & (num % 1 == 0)
        whole = num.where(integral).astype("Int64").astype("string")
        canon = whole.fillna(num.astype("string"))
    else:
        # Object/string: keep the literal text so "007" stays "007" (no numeric coercion).
        canon = series.astype("string")
    # Use real None (not pd.NA) for nulls so equality yields plain bool masks, never NA -
    # an NA-valued mask would make `Series.where` treat null rows as matches.
    result = canon.astype(object)
    result[canon.isna()] = None
    return result
