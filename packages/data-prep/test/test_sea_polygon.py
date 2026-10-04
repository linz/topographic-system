from pathlib import Path

import geopandas as gpd
from data_prep.sea_polygon import NZGD2000, SLICE_ZOOM, run
from shapely.geometry import box

# A land polygon over New Zealand
LAND_POLYGON = box(174.7, -41.4, 174.9, -41.2)


def run_sea_polygon(tmp_path: Path, land_polygons, zoom: int = SLICE_ZOOM) -> gpd.GeoDataFrame:
    tmp_path.mkdir(parents=True, exist_ok=True)
    land_gdf = gpd.GeoDataFrame(
        {"created_at": ["2020-01-01"] * len(land_polygons), "geometry": land_polygons},
        crs=NZGD2000,
    )
    land_path = tmp_path / "land.parquet"
    land_gdf.to_parquet(land_path)

    output_path = tmp_path / "output.parquet"
    run(land_path, output_path, zoom)
    return gpd.read_parquet(output_path)


def test_produces_sea_polygons(tmp_path: Path):
    result = run_sea_polygon(tmp_path, [LAND_POLYGON])
    assert not result.empty
    assert (result["type"] == "moana").all()


def test_output_is_nzgd2000(tmp_path: Path):
    result = run_sea_polygon(tmp_path, [LAND_POLYGON])
    assert result.crs is not None
    assert result.crs.to_epsg() == NZGD2000


def test_geometries_are_single_polygons(tmp_path: Path):
    result = run_sea_polygon(tmp_path, [LAND_POLYGON])
    assert (result.geometry.geom_type == "Polygon").all()


def test_ids_are_unique(tmp_path: Path):
    result = run_sea_polygon(tmp_path, [LAND_POLYGON])
    assert result["id"].is_unique


def test_ids_are_reproducible(tmp_path: Path):
    first = run_sea_polygon(tmp_path, [LAND_POLYGON])
    second = run_sea_polygon(tmp_path, [LAND_POLYGON])
    assert set(first["id"]) == set(second["id"])


def test_quadkeys_are_present(tmp_path: Path):
    result = run_sea_polygon(tmp_path, [LAND_POLYGON])
    # A tile with disconnected sea yields several single-part polygons that share
    # a quadkey, so quadkeys are not unique; they must always be populated though.
    assert result["quadkey"].notna().all()


def test_sea_excludes_land(tmp_path: Path):
    result = run_sea_polygon(tmp_path, [LAND_POLYGON])
    land = gpd.GeoSeries([LAND_POLYGON], crs=NZGD2000).to_crs(2193).iloc[0]
    sea = result.to_crs(2193).geometry.union_all()
    # The sea must not cover the land (allowing for tiny precision slivers).
    assert sea.intersection(land).area < land.area * 1e-6


def test_higher_zoom_produces_more_tiles(tmp_path: Path):
    # A land polygon spanning several degrees so it overlaps many tiles.
    wide_land = box(172.0, -44.0, 176.0, -40.0)
    coarse = run_sea_polygon(tmp_path / "coarse", [wide_land], zoom=6)
    fine = run_sea_polygon(tmp_path / "fine", [wide_land], zoom=8)
    assert len(fine) > len(coarse)
