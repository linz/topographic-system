"""Type-strict value matching for config-driven transforms.

Config values come from YAML, where the literal type is significant: ``1`` is an int,
``'1'`` a string. Columns come from pyogrio. Corrections match on the *raw* value and
refuse to coerce across type categories - an integer column compared against a YAML string
key is a config/data error we surface rather than silently matching nothing.

This is the single home for the type-category matching rule shared by `corrections` (and,
once merged, lookup joins) - prefer it over ad-hoc ``astype(str)`` or ``to_numeric``.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


def value_category(value: Any) -> str:
    """Coarse type bucket for a column (Series) or a scalar config value.

    Two values can only be matched when their categories are equal. The buckets mirror the
    join-key categories so the two matching rules stay aligned.
    """
    if isinstance(value, pd.Series):
        dtype = value.dtype
        if pd.api.types.is_bool_dtype(dtype):
            return "bool"
        if pd.api.types.is_integer_dtype(dtype):
            return "integer"
        if pd.api.types.is_float_dtype(dtype):
            return "float"
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return "datetime"
        if pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype):
            return "string"
        return dtype.name
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "null"
    # bool is a subclass of int, so it must be checked first.
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "string"
    return type(value).__name__


def require_compatible(column: pd.Series, value: Any, *, column_name: str, dataset: str) -> None:
    """Raise ``TypeError`` unless config `value` can match `column` without coercion.

    A null key matches nothing and so clashes with nothing; an empty column has no values to
    clash with either. Both are compatible.
    """
    value_cat = value_category(value)
    if value_cat == "null" or column.empty:
        return
    column_cat = value_category(column)
    if column_cat != value_cat:
        raise TypeError(
            f"correction type mismatch in {dataset}: column '{column_name}' is "
            f"{column.dtype} ({column_cat}) but config value {value!r} is {value_cat}; "
            f"align the config value's type with the column"
        )
