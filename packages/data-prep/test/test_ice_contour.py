from datetime import date
from pathlib import Path

import geopandas as gpd
import pytest
from data_prep.ice_contour import NZGD2000, run
from shapely.geometry import Polygon


@pytest.fixture()
def result(tmp_path: Path):
    poly1 = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
    poly2 = Polygon([(1, 1), (3, 1), (3, 3), (1, 3)])

    contour_gdf = gpd.GeoDataFrame(
        {
            "topo_id": [1],
            "elevation": [100],
            "definition": [None],
            "designation": [None],
            "formation": [None],
            "geometry": [poly1],
        },
        crs=NZGD2000,
    )

    landcover_gdf = gpd.GeoDataFrame(
        {
            "id": [10],
            "type": ["ice"],
            "created_at": [date(2025, 1, 2)],
            "updated_at": [date(2025, 6, 15)],
            "geometry": [poly2],
        },
        crs=NZGD2000,
    )

    contour_path = tmp_path / "contour.parquet"
    landcover_path = tmp_path / "landcover.parquet"
    output_path = tmp_path / "output.parquet"

    contour_gdf.to_parquet(contour_path)
    landcover_gdf.to_parquet(landcover_path)

    run(contour_path, landcover_path, output_path)

    return gpd.read_parquet(output_path)


def test_geometry_is_intersection(result):
    expected = Polygon([(1, 1), (2, 1), (2, 2), (1, 2)])
    assert result.iloc[0].geometry.equals(expected)


def test_updated_at_takes_landcover(result):
    assert result.iloc[0]["updated_at"] == date(2025, 6, 15)
