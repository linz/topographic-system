import geopandas as gpd
import pandas as pd
import pyogrio
import pytest
from shapely.geometry import Point

from . import theme
from .theme import coerce_dtypes, unify_dtypes, untyped_columns


def _gdf(**columns) -> gpd.GeoDataFrame:
    length = len(next(iter(columns.values())))
    return gpd.GeoDataFrame(columns, geometry=[Point(0, 0)] * length, crs="EPSG:4167")


def _merge(gdfs: list[gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
    """What `merge_theme_release` does between unifying and coercing."""
    unify_dtypes(gdfs)
    return gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)


@pytest.fixture
def schema(monkeypatch):
    """Stand in for `schema/<theme>.json`: `schema(t50_fid="Int64")`, or `schema()` for none."""

    def _apply(**dtypes: str) -> None:
        monkeypatch.setattr(theme, "schema_dtypes", lambda name: dtypes)

    return _apply


# An all-NULL column reaches the merge in two shapes: as the mapping wrote it, and as parquet
# returns it. The float64 NaN additionally cannot be cast to a nullable integer, so `_cast` has to
# rebuild it as typed NULLs rather than convert it.
ALL_NULL_SHAPES = {
    "object None": [None],
    "float64 NaN": pd.array([float("nan")], dtype="float64"),
}


@pytest.mark.parametrize("all_null", ALL_NULL_SHAPES.values(), ids=ALL_NULL_SHAPES)
def test_all_null_column_takes_the_dtype_of_its_siblings(all_null):
    """The airport bug: `t50_fid: null` in one dataset made the merged column object, and an
    object column is written as OFTString, so integers reached kart as text."""
    populated = _gdf(t50_fid=pd.array([4915871, 4915872], dtype="int32"))

    merged = _merge([populated, _gdf(t50_fid=all_null)])

    assert merged["t50_fid"].dtype == "Int32"
    assert merged["t50_fid"].tolist()[:2] == [4915871, 4915872]
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


def test_coerce_applies_the_schema_dtypes(schema):
    schema(t50_fid="Int64", height="Float64")
    merged = _gdf(t50_fid=pd.array([1.0, 2.0], dtype="float64"), height=pd.array([3, 4], dtype="int32"))

    coerced = coerce_dtypes(merged, "water_point")

    assert coerced["t50_fid"].dtype == "Int64"
    assert coerced["height"].dtype == "Float64"


def test_dtype_does_not_depend_on_which_release_is_merged(schema):
    """The same column must reach kart as one type for every release in its history.

    Kart takes the field type from the file it imports, so a column that is Int64 in one release
    and Int32 in the next is a schema change partway through the dataset's history. These three
    releases legitimately differ in what the sources carried, and must still coerce alike -- which
    also pins that an Int32 merely *satisfying* an `integer` schema is widened, not left alone.
    """
    schema(t50_fid="Int64")
    releases = {
        "before since_release": [_gdf(t50_fid=[None]), _gdf(t50_fid=[None])],
        "narrow source only": [_gdf(t50_fid=pd.array([1], dtype="int32")), _gdf(t50_fid=[None])],
        "wider source added": [
            _gdf(t50_fid=pd.array([1], dtype="int32")),
            _gdf(t50_fid=pd.array([2**40], dtype="int64")),
        ],
    }

    dtypes = {name: coerce_dtypes(_merge(gdfs), "airport")["t50_fid"].dtype for name, gdfs in releases.items()}

    assert dtypes == dict.fromkeys(releases, pd.Int64Dtype())


# pandas refuses a lossy `float64` -> Int64 cast itself, but silently truncates a nullable `Float64`
# one -- and the unify pass always produces the nullable form, so `_cast` makes that check instead.
FRACTIONAL = {
    "float64": ([1.5], r"cannot cast float64 to Int64"),
    "Float64": (pd.array([1.5, 2.5], dtype="Float64"), r"would truncate non-integer values"),
}


@pytest.mark.parametrize(("values", "message"), FRACTIONAL.values(), ids=FRACTIONAL)
def test_coerce_rejects_fractional_values_for_an_integer_column(schema, values, message):
    """A fractional t50_fid is a data defect: fail here rather than let it truncate silently or
    degrade to text downstream."""
    schema(t50_fid="Int64")

    with pytest.raises(ValueError, match=rf"airport\.t50_fid: .*{message}"):
        coerce_dtypes(_gdf(t50_fid=values), "airport")


INTEGRAL = {
    "float64": ([4915871.0, 4915872.0], [4915871, 4915872]),
    "Float64 with NULL": (pd.array([4915871.0, None], dtype="Float64"), [4915871, pd.NA]),
}


@pytest.mark.parametrize(("values", "expected"), INTEGRAL.values(), ids=INTEGRAL)
def test_coerce_accepts_integral_floats_for_an_integer_column(schema, values, expected):
    """The strictness above must not become "reject all floats": pyogrio reads an integer column
    that holds a NULL as float, and those values are integral, so they convert losslessly."""
    schema(t50_fid="Int64")

    coerced = coerce_dtypes(_gdf(t50_fid=values), "airport")

    assert coerced["t50_fid"].dtype == "Int64"
    assert coerced["t50_fid"].tolist() == expected


def test_coerce_types_a_column_that_is_null_everywhere(schema):
    """`unify_dtypes` has no evidence for such a column, so only the schema can type it."""
    schema(metadata="string")

    coerced = coerce_dtypes(_gdf(metadata=[None, None]), "airport")

    assert coerced["metadata"].dtype == "string"
    assert coerced["metadata"].isna().all()


def test_untyped_columns_are_reported(schema):
    """A column no pass could type is written as text. It must not pass silently: the same column
    types properly in a later release where a value appears, changing the kart schema mid-history.
    """
    schema()

    merged = coerce_dtypes(_gdf(t50_fid=[None], name=[None]), "airport")

    assert untyped_columns(merged) == ["t50_fid", "name"]


def test_coerced_and_geometry_columns_are_not_reported(schema):
    """Only genuinely untyped columns count -- not ones the schema settled, and never geometry."""
    schema(t50_fid="Int64")

    merged = coerce_dtypes(_gdf(t50_fid=[None], name=pd.array(["a"], dtype="string")), "airport")

    assert untyped_columns(merged) == []


@pytest.mark.parametrize(
    "value",
    ["2015-11-19T02:26:49+00:00", "2015-11-19T02:26:49Z", "2015-11-19T15:26:49+13:00"],
)
def test_lifecycle_columns_are_normalised_to_rfc3339(schema, value):
    """`format: date-time` needs an offset."""
    schema()

    coerced = coerce_dtypes(_gdf(created_at=[value], updated_at=[value]), "airport")

    assert coerced["created_at"].tolist() == ["2015-11-19T02:26:49Z"]
    assert coerced["updated_at"].tolist() == ["2015-11-19T02:26:49Z"]
    assert coerced["created_at"].dtype == "string"


def test_lifecycle_columns_are_normalised_without_a_schema(schema):
    """A schema-less dev run has to emit the same timestamps as a run with one."""
    schema()

    coerced = coerce_dtypes(_gdf(created_at=["2015-11-19T02:26:49+00:00"], t50_fid=[1.5]), "no_schema")

    assert coerced["created_at"].tolist() == ["2015-11-19T02:26:49Z"]
    assert coerced["t50_fid"].tolist() == [1.5]  # unconstrained without a schema, so untouched


@pytest.mark.parametrize(
    "gdfs",
    [
        pytest.param([_gdf(t50_fid=[None, None])], id="all NULL"),
        pytest.param([_gdf(t50_fid=pd.array([1], dtype="int32")), _gdf(t50_fid=[None])], id="narrow source"),
    ],
)
def test_integer_column_reaches_the_file_as_int64(tmp_path, schema, gdfs):
    """The dtype has to survive into the file, not just the frame -- the file is what kart takes
    the field type from. pyogrio writes an `object` column as OFTString and an Int32 as OFTInteger,
    so either would commit a type the schema did not ask for."""
    schema(t50_fid="Int64")
    output = tmp_path / "airport.fgb"

    coerce_dtypes(_merge(gdfs), "airport").to_file(output, driver="FlatGeobuf", index=False)

    info = pyogrio.read_info(output)
    assert dict(zip(info["fields"], info["dtypes"], strict=True))["t50_fid"] == "int64"


def test_missing_transform_names_the_datasets(tmp_path, monkeypatch):
    """The previous stage hasn't produced everything this one needs -- the same failure transform
    itself raises for a missing input, and it says which datasets to go and build."""
    monkeypatch.setattr(theme, "WORKING_TRANSFORM_DIR", tmp_path / "transform")
    monkeypatch.setattr(theme, "WORKING_THEME_DIR", tmp_path / "theme")

    with pytest.raises(FileNotFoundError, match=r"airport release 53: no transform output for .*Run transform first"):
        theme.merge_theme_release("airport", 53)
