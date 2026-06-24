import pandas as pd

from .match import normalise


def test_scalar_int_float_string_all_canonicalise_equal():
    # The core promise: 5, 5.0 and "5" are the same key.
    assert normalise(5) == normalise(5.0) == normalise("5") == "5"


def test_scalar_leading_zero_string_is_preserved():
    assert normalise("007") == "007"


def test_scalar_non_integral_float_keeps_full_form():
    assert normalise(2.5) == "2.5"


def test_scalar_none_is_none():
    assert normalise(None) is None
    assert normalise(float("nan")) is None


def test_float_column_matches_string_config_value():
    # pyogrio may read an int id column as float when nulls widen it.
    col = pd.Series([1.0, 2.0, 3.0])
    assert (normalise(col) == normalise("1")).tolist() == [True, False, False]


def test_int_column_matches_string_config_value():
    col = pd.Series([1, 2, 3])
    assert (normalise(col) == normalise("2")).tolist() == [False, True, False]


def test_string_column_with_leading_zeros_not_collapsed():
    col = pd.Series(["007", "012", "7"])
    assert normalise(col).tolist() == ["007", "012", "7"]
    assert (normalise(col) == normalise("007")).tolist() == [True, False, False]


def test_null_rows_canonicalise_to_none_and_never_match():
    col = pd.Series([1.0, None, 3.0])
    canon = normalise(col)
    assert canon.tolist() == ["1", None, "3"]
    # A null cell yields a plain-False mask, never NA (which would corrupt `Series.where`).
    mask = canon == normalise("1")
    assert mask.dtype == bool
    assert mask.tolist() == [True, False, False]


def test_mixed_integral_and_non_integral_float_column():
    col = pd.Series([1.0, 2.5, 3.0])
    assert normalise(col).tolist() == ["1", "2.5", "3"]
