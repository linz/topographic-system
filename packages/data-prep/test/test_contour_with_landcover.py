from pathlib import Path

import geopandas as gpd
from shapely.geometry import Polygon

from contour_with_landcover import run


def test_contour_with_landcover(tmp_path: Path):
    poly1 = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
    poly2 = Polygon([(1, 1), (3, 1), (3, 3), (1, 3)])

    contour_gdf = gpd.GeoDataFrame(
        {
            "feature_type": ["contour"],
            "topo_id": [1],
            "geometry": [poly1],
        },
    )

    landcover_gdf = gpd.GeoDataFrame(
        {
            "feature_type": ["forest"],
            "topo_id": [10],
            "geometry": [poly2],
        },
    )

    contour_path = tmp_path / "contour.parquet"
    landcover_path = tmp_path / "landcover.parquet"
    output_path = tmp_path / "output.parquet"

    contour_gdf.to_parquet(contour_path)
    landcover_gdf.to_parquet(landcover_path)

    run(contour_path, landcover_path, output_path)

    result = gpd.read_parquet(output_path)

    assert not result.empty
    assert "feature_type" in result.columns
    assert "topo_id" in result.columns
    assert "landcover_feature_type" in result.columns
    assert "landcover_topo_id" in result.columns
