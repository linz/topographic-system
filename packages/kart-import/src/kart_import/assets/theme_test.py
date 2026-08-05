import geopandas as gpd
import pandas as pd
import pyogrio
import pytest
from shapely.geometry import Point

from . import theme
from .theme import coerce_dtypes, unify_dtypes


def _gdf(**columns) -> gpd.GeoDataFrame:
    length = len(next(iter(columns.values())))
    return gpd.GeoDataFrame(columns, geometry=[Point(0, 0)] * length, crs="EPSG:4167")


def _merge(gdfs: list[gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
    """What `merge_theme_release` does between unifying and coercing."""
    unify_dtypes(gdfs)
    return gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)


def test_all_null_column_takes_the_dtype_of_its_siblings():
    """The airport bug: `t50_fid: null` in one dataset made the merged column object, and an
    object column is written as OFTString, so integers reached kart as text."""
    populated = _gdf(t50_fid=pd.array([4915871, 4915872], dtype="int32"))
    all_null = _gdf(t50_fid=[None])

    merged = _merge([populated, all_null])

    assert merged["t50_fid"].dtype == "Int32"
    assert merged["t50_fid"].tolist() == [4915871, 4915872, pd.NA]


def test_all_null_column_survives_the_parquet_float_shape():
    """An all-NULL column comes back from parquet as float64 NaN, which pandas refuses to cast
    to a nullable integer. It has to be rebuilt as typed NULLs rather than cast."""
    populated = _gdf(t50_fid=pd.array([1, 2], dtype="int32"))
    all_null = _gdf(t50_fid=pd.array([float("nan")], dtype="float64"))

    merged = _merge([populated, all_null])

    assert merged["t50_fid"].dtype == "Int32"
    assert merged["t50_fid"].isna().tolist() == [False, False, True]


def test_float_sources_are_not_narrowed_to_integer():
    """Regression: the common dtype was read off *empty* series, and `convert_dtypes` calls an
    empty float column Int64, which then rejects the real float values."""
    merged = _merge([_gdf(orientation=[1.5, 2.5]), _gdf(orientation=[None])])

    assert merged["orientation"].dtype == "Float64"
    assert merged["orientation"].tolist()[:2] == [1.5, 2.5]


def test_disagreeing_sources_are_left_for_the_schema():
    """No safe common type between text and int, so `unify_dtypes` declines to invent one."""
    merged = _merge([_gdf(code=pd.array(["a"], dtype="string")), _gdf(code=pd.array([1], dtype="int32"))])

    assert merged["code"].dtype == object


def test_coerce_applies_the_schema_dtypes(monkeypatch):
    monkeypatch.setattr(theme, "schema_dtypes", lambda name: {"t50_fid": "Int64", "height": "Float64"})
    merged = _gdf(t50_fid=pd.array([1.0, 2.0], dtype="float64"), height=pd.array([3, 4], dtype="int32"))

    coerced = coerce_dtypes(merged, "water_point")

    assert coerced["t50_fid"].dtype == "Int64"
    assert coerced["height"].dtype == "Float64"


def test_coerce_keeps_a_narrower_dtype_that_already_carries_the_type(monkeypatch):
    """Int32 from the sources satisfies an `integer` schema; widening it buys nothing."""
    monkeypatch.setattr(theme, "schema_dtypes", lambda name: {"t50_fid": "Int64"})

    coerced = coerce_dtypes(_gdf(t50_fid=pd.array([1], dtype="Int32")), "airport")

    assert coerced["t50_fid"].dtype == "Int32"


def test_coerce_rejects_a_value_the_schema_type_cannot_hold(monkeypatch):
    """A fractional t50_fid is a data defect: fail here rather than let it degrade to text."""
    monkeypatch.setattr(theme, "schema_dtypes", lambda name: {"t50_fid": "Int64"})

    with pytest.raises(ValueError, match=r"airport\.t50_fid: cannot cast float64 to Int64"):
        coerce_dtypes(_gdf(t50_fid=[1.5]), "airport")


def test_coerce_accepts_an_exactly_representable_float(monkeypatch):
    """The strictness above must not become "reject all floats": a source column that is
    float-typed but integral (4915871.0) is a real case and converts losslessly."""
    monkeypatch.setattr(theme, "schema_dtypes", lambda name: {"t50_fid": "Int64"})

    coerced = coerce_dtypes(_gdf(t50_fid=[4915871.0, 4915872.0]), "airport")

    assert coerced["t50_fid"].dtype == "Int64"
    assert coerced["t50_fid"].tolist() == [4915871, 4915872]


@pytest.mark.parametrize(
    "value",
    ["2015-11-19T02:26:49+00:00", "2015-11-19T02:26:49Z", "2015-11-19T15:26:49+13:00"],
)
def test_lifecycle_columns_are_normalised_to_rfc3339(monkeypatch, value):
    """`format: date-time` needs an offset."""
    monkeypatch.setattr(theme, "schema_dtypes", lambda name: {})

    coerced = coerce_dtypes(_gdf(created_at=[value], updated_at=[value]), "airport")

    assert coerced["created_at"].tolist() == ["2015-11-19T02:26:49Z"]
    assert coerced["updated_at"].tolist() == ["2015-11-19T02:26:49Z"]
    assert coerced["created_at"].dtype == "string"


def test_lifecycle_columns_are_normalised_without_a_schema(monkeypatch):
    """A schema-less dev run has to emit the same timestamps as a run with one."""
    monkeypatch.setattr(theme, "schema_dtypes", lambda name: {})

    coerced = coerce_dtypes(_gdf(created_at=["2015-11-19T02:26:49+00:00"], t50_fid=[1.5]), "no_schema")

    assert coerced["created_at"].tolist() == ["2015-11-19T02:26:49Z"]
    assert coerced["t50_fid"].tolist() == [1.5]  # unconstrained without a schema, so untouched


def test_coerce_types_a_column_that_is_null_everywhere(monkeypatch):
    """`unify_dtypes` has no evidence for such a column, so only the schema can type it."""
    monkeypatch.setattr(theme, "schema_dtypes", lambda name: {"metadata": "string"})

    coerced = coerce_dtypes(_gdf(metadata=[None, None]), "airport")

    assert coerced["metadata"].dtype == "string"
    assert coerced["metadata"].isna().all()


def test_all_null_integer_survives_the_write(tmp_path, monkeypatch):
    """The other end of the airport bug: the dtype has to survive into the file, not just the
    frame. Left as `object` the column is written as OFTString and kart records text."""
    monkeypatch.setattr(theme, "schema_dtypes", lambda name: {"t50_fid": "Int64"})
    output = tmp_path / "airport.fgb"

    coerce_dtypes(_gdf(t50_fid=[None, None]), "airport").to_file(output, driver="FlatGeobuf", index=False)

    info = pyogrio.read_info(output)
    assert dict(zip(info["fields"], info["dtypes"], strict=True))["t50_fid"] == "int64"


def test_missing_transform_names_the_datasets(tmp_path, monkeypatch):
    """The previous stage hasn't produced everything this one needs -- the same failure transform
    itself raises for a missing input, and it says which datasets to go and build."""
    monkeypatch.setattr(theme, "WORKING_TRANSFORM_DIR", tmp_path / "transform")
    monkeypatch.setattr(theme, "WORKING_THEME_DIR", tmp_path / "theme")

    with pytest.raises(FileNotFoundError, match=r"airport release 53: no transform output for .*Run transform first"):
        theme.merge_theme_release("airport", 53)
