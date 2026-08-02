import pandas as pd
import pytest

from .match import require_compatible, value_category


def test_scalar_categories():
    assert value_category(5) == "integer"
    assert value_category(5.0) == "float"
    assert value_category("5") == "string"
    assert value_category(True) == "bool"  # bool before int
    assert value_category(None) == "null"
    assert value_category(float("nan")) == "null"


def test_series_categories():
    assert value_category(pd.Series([1, 2, 3])) == "integer"
    assert value_category(pd.Series([1.0, 2.0])) == "float"
    assert value_category(pd.Series(["a", "b"])) == "string"
    assert value_category(pd.Series([1, None], dtype="Int64")) == "integer"
    assert value_category(pd.Series([1, None])) == "float"  # pandas widens these by default
    assert value_category(pd.Series([True, False])) == "bool"
    assert value_category(pd.Series(pd.to_datetime(["2020-01-01", "2020-01-02"]))) == "datetime"


def test_require_compatible_passes_for_matching_categories():
    require_compatible(pd.Series([1, 2, 3]), 2, column_name="c", dataset="t")
    require_compatible(pd.Series([1.0, 2.0]), 1.0, column_name="c", dataset="t")
    require_compatible(pd.Series(["a", "b"]), "a", column_name="c", dataset="t")
    require_compatible(pd.Series([True, False]), True, column_name="c", dataset="t")


def test_require_compatible_raises_on_mismatch():
    with pytest.raises(TypeError, match="type mismatch.*column 'c' is.*integer.*config value '1' is string"):
        require_compatible(pd.Series([1, 2, 3]), "1", column_name="c", dataset="t")


def test_require_compatible_distinguishes_int_from_float():
    with pytest.raises(TypeError, match="float.*config value 1 is integer"):
        require_compatible(pd.Series([1.0, 2.0]), 1, column_name="c", dataset="t")


def test_require_compatible_distinguishes_bool_from_int():
    # `True == 1` in pandas, so without the category guard a bool column would silently
    # match an int key. The guard must reject it.
    with pytest.raises(TypeError, match="bool.*config value 1 is integer"):
        require_compatible(pd.Series([True, False]), 1, column_name="c", dataset="t")


def test_require_compatible_allows_null_value_and_empty_column():
    # A null key clashes with nothing; an empty column has nothing to clash with.
    require_compatible(pd.Series([1, 2, 3]), None, column_name="c", dataset="t")
    require_compatible(pd.Series([], dtype="object"), "anything", column_name="c", dataset="t")
