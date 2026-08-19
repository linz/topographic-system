from pathlib import Path

import geopandas as gpd
import pytest
from data_prep.carto_symbol import run
from data_prep.parquet_utils import NZGD2000, NZTM2000
from shapely.geometry import LineString, Point, box


@pytest.fixture
def result(tmp_path: Path) -> gpd.GeoDataFrame:
    input_path = tmp_path / "input.gpkg"

    layers = {
        "contour_number": gpd.GeoDataFrame(
            {"orientation": [0], "cont_rota": [0], "geometry": [Point(174, -41)]},
            crs=NZGD2000,
        ),
        "linz_highway_sh": gpd.GeoDataFrame({"geometry": [Point(174, -42)]}, crs=NZGD2000),
        "golf_sym": gpd.GeoDataFrame({"geometry": [Point(175, -43)]}, crs=NZGD2000),
        "mine_sym": gpd.GeoDataFrame({"mine_vis": ["opencast"], "geometry": [Point(175, -46)]}, crs=NZGD2000),
    }
    for index, (layer, gdf) in enumerate(layers.items()):
        gdf.to_crs(NZTM2000).to_file(input_path, layer=layer, driver="GPKG", mode="w" if index == 0 else "a")

    contour_gdf = gpd.GeoDataFrame(
        {
            "id": ["1"],
            "elevation": [100],
            "geometry": [LineString([(174, -41), (175, -41)])],
        },
        crs=NZGD2000,
    )

    road_line_gdf = gpd.GeoDataFrame(
        {
            "id": ["2"],
            "highway_number": ["5"],
            "geometry": [LineString([(174, -42), (175, -42)])],
        },
        crs=NZGD2000,
    )

    landuse_gdf = gpd.GeoDataFrame(
        {
            "id": ["3", "4"],
            "geometry": [box(174, -42, 176, -44), box(174, -45, 176, -47)],
        },
        crs=NZGD2000,
    )

    contour_path = tmp_path / "contour.parquet"
    road_line_path = tmp_path / "road_line.parquet"
    landuse_path = tmp_path / "landuse.parquet"
    output_path = tmp_path / "output.parquet"

    contour_gdf.to_parquet(contour_path)
    road_line_gdf.to_parquet(road_line_path)
    landuse_gdf.to_parquet(landuse_path)

    run(input_path, contour_path, road_line_path, landuse_path, output_path)
    return gpd.read_parquet(output_path)


def test_all_symbols_present(result):
    assert len(result) == 4
    assert set(result["type"]) == {"contour_number", "highway_shield", "golf_course", "mine_opencast"}


def test_symbols_keep_source_id_of_matched_feature(result):
    source_ids = result.set_index("type")["source_id"]
    assert source_ids["contour_number"] == "1"
    assert source_ids["highway_shield"] == "2"
    assert source_ids["golf_course"] == "3"
    assert source_ids["mine_opencast"] == "4"
