from pathlib import Path

import geopandas as gpd
import pytest
from data_prep.ice_contour import run
from data_prep.parquet_utils import NZGD2000
from shapely.geometry import LineString, box


@pytest.fixture()
def result(tmp_path: Path):
    contour_gdf = gpd.GeoDataFrame(
        {
            "id": [1],
            "elevation": [100],
            "definition": [None],
            "designation": [None],
            "formation": [None],
            "metadata": [None],
            "geometry": [LineString([(174, -41), (178, -41)])],
        },
        crs=NZGD2000,
    )

    landcover_gdf = gpd.GeoDataFrame(
        {
            "id": [10],
            "type": ["ice"],
            "created_at": ["2025-01-02"],
            "updated_at": ["2025-06-15"],
            "geometry": [box(175, -42, 177, -40)],
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
    assert result.iloc[0].geometry.equals(LineString([(175, -41), (177, -41)]))


def test_updated_at_takes_landcover(result):
    assert result.iloc[0]["updated_at"] == "2025-06-15"
