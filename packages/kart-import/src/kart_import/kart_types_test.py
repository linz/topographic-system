import numpy as np
import pandas as pd
import pytest

from .kart_types import INT_DTYPE, coerce_integer_columns, integer_columns

# Shaped after `linz_road_cl` and its siblings: integer pk, exact `numeric(n, 0)` id, scaled
# numeric, text.
SCHEMA = [
    {"name": "auto_pk", "dataType": "integer", "size": 64},
    {"name": "geom", "dataType": "geometry", "geometryType": "MULTILINESTRING"},
    {"name": "t50_fid", "dataType": "numeric", "precision": 10, "scale": 0},
    {"name": "lol_sufi", "dataType": "numeric", "precision": 20, "scale": 0},
    {"name": "text_heigh", "dataType": "numeric", "precision": 12, "scale": 6},
    {"name": "unscaled", "dataType": "numeric", "precision": 8},
    {"name": "unconstrained", "dataType": "numeric"},
    {"name": "width", "dataType": "text", "length": 1},
]


def test_integer_columns_takes_integers_and_exact_numerics_only():
    # `lol_sufi` is in despite precision 20: precision decides nothing, values do.
    # `unscaled` is in because a bare DECIMAL(p) is DECIMAL(p, 0); `unconstrained` has neither
    # precision nor scale, so nothing says it is whole.
    assert integer_columns(SCHEMA) == {"auto_pk", "t50_fid", "lol_sufi", "unscaled"}


def test_coerce_restores_a_declared_integer_that_arrived_as_float():
    """The whole point: `numeric(10, 0)` exported as REAL, put back."""
    df = pd.DataFrame({"t50_fid": [3197173.0, 7874815.0], "width": ["A", "B"]})

    out = coerce_integer_columns(df, SCHEMA, context="ctx")

    assert out["t50_fid"].dtype == INT_DTYPE
    assert out["t50_fid"].tolist() == [3197173, 7874815]
    assert out["width"].tolist() == ["A", "B"]  # undeclared-as-integer column untouched
    assert df["t50_fid"].dtype == np.dtype("float64")  # caller's frame not mutated


def test_coerce_keeps_the_nulls_that_widened_the_column():
    """A nullable OGR integer is float64 only because of its NULLs; the cast must keep them."""
    df = pd.DataFrame({"auto_pk": [1.0, np.nan, 3.0]})

    out = coerce_integer_columns(df, SCHEMA, context="ctx")

    assert out["auto_pk"].dtype == INT_DTYPE
    assert out["auto_pk"].isna().tolist() == [False, True, False]
    assert out["auto_pk"].dropna().tolist() == [1, 3]


def test_coerce_leaves_alone_what_it_should():
    """Already-integer columns and scaled numerics keep their dtype."""
    df = pd.DataFrame(
        {
            "auto_pk": pd.Series([1, 2], dtype="int64"),  # declared int, already int
            "text_heigh": pd.Series([1.5, 2.25], dtype="float64"),  # numeric(12, 6) - genuinely fractional
        }
    )

    out = coerce_integer_columns(df, SCHEMA, context="ctx")

    assert out is df  # nothing to do -> no copy
    assert out["auto_pk"].dtype == np.dtype("int64")
    assert out["text_heigh"].dtype == np.dtype("float64")


@pytest.mark.parametrize(
    "values, match",
    [
        ([1.5, 2.0], "fractional values"),  # not the whole number it was declared
        ([2.0**53, 2.0], "exact-integer limit"),  # may already have rounded to a different id
    ],
    ids=["fractional", "beyond-exact-int-range"],
)
def test_coerce_raises_where_values_contradict_the_declaration(values, match):
    df = pd.DataFrame({"t50_fid": values})
    with pytest.raises(ValueError, match=match):
        coerce_integer_columns(df, SCHEMA, context="lookup 'road_lkp' at abc123")


def test_coerce_error_names_its_context_and_column():
    """Which lookup and commit, or the message is unactionable in a bulk run."""
    with pytest.raises(ValueError, match=r"lookup 'road_lkp' at abc123: column 't50_fid'"):
        coerce_integer_columns(pd.DataFrame({"t50_fid": [1.5]}), SCHEMA, context="lookup 'road_lkp' at abc123")


def test_coerce_restores_a_numeric_declared_without_a_scale():
    """A bare DECIMAL(p) is DECIMAL(p, 0); skipping it would leave the float in place."""
    out = coerce_integer_columns(pd.DataFrame({"unscaled": [7.0, 9.0]}), SCHEMA, context="ctx")

    assert out["unscaled"].dtype == INT_DTYPE
    assert out["unscaled"].tolist() == [7, 9]
