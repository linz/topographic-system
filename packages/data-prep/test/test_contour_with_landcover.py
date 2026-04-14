from datetime import date
from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from data_prep.contour_with_landcover import run


@pytest.fixture()
def result(tmp_path: Path):
    poly1 = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
    poly2 = Polygon([(1, 1), (3, 1), (3, 3), (1, 3)])

    contour_gdf = gpd.GeoDataFrame(
        {
            "feature_type": ["contour"],
            "topo_id": [1],
            "update_date": [date(2024, 1, 1)],
            "version": [3],
            "geometry": [poly1],
        },
    )

    landcover_gdf = gpd.GeoDataFrame(
        {
            "feature_type": ["ice"],
            "topo_id": [10],
            "update_date": [date(2025, 6, 15)],
            "version": [1],
            "geometry": [poly2],
        },
    )

    contour_path = tmp_path / "contour.parquet"
    landcover_path = tmp_path / "landcover.parquet"
    output_path = tmp_path / "output.parquet"

    contour_gdf.to_parquet(contour_path)
    landcover_gdf.to_parquet(landcover_path)

    run(contour_path, landcover_path, output_path)

    return gpd.read_parquet(output_path)


def test_output_has_expected_columns(result):
    assert "feature_type" in result.columns
    assert "topo_id" in result.columns
    assert "landcover_id" in result.columns
    assert "landcover_feature_type" in result.columns
    assert "update_date" in result.columns
    assert "version" in result.columns


def test_landcover_feature_type_is_ice(result):
    assert not result.empty
    assert (result["landcover_feature_type"] == "ice").all()


def test_landcover_id(result):
    assert result.iloc[0]["landcover_id"] == 10


def test_update_date_takes_landcover(result):
    assert result.iloc[0]["update_date"] == date(2025, 6, 15)


def test_version_takes_landcover(result):
    assert result.iloc[0]["version"] == 1


def test_geometry_is_intersection(result):
    expected = Polygon([(1, 1), (2, 1), (2, 2), (1, 2)])
    assert result.iloc[0].geometry.equals(expected)
