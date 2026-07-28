from datetime import date
from pathlib import Path

import geopandas as gpd
import pytest
from data_prep.sea_polygon import NZGD2000, SLICE_ZOOM, run, tile_bounds, tile_quadkey, tile_range
from shapely.geometry import box

# A land polygon over New Zealand, in NZGD2000 (lon/lat) so no reprojection is
# needed to build the fixture. Small enough that it does not fill a whole tile.
LAND_POLYGON = box(174.7, -41.4, 174.9, -41.2)


def run_sea_polygon(tmp_path: Path, land_polygons, zoom: int = SLICE_ZOOM) -> gpd.GeoDataFrame:
    tmp_path.mkdir(parents=True, exist_ok=True)
    land_gdf = gpd.GeoDataFrame(
        {"created_at": [date(2020, 1, 1)] * len(land_polygons), "geometry": land_polygons},
        crs=NZGD2000,
    )
    land_path = tmp_path / "land.parquet"
    land_gdf.to_parquet(land_path)

    output_path = tmp_path / "output.parquet"
    run(land_path, output_path, zoom)
    return gpd.read_parquet(output_path)


def test_produces_moana_polygons(tmp_path: Path):
    result = run_sea_polygon(tmp_path, [LAND_POLYGON])
    assert not result.empty
    assert (result["type"] == "moana").all()
    assert result.geometry.geom_type.isin(["Polygon", "MultiPolygon"]).all()


def test_output_is_nzgd2000(tmp_path: Path):
    result = run_sea_polygon(tmp_path, [LAND_POLYGON])
    assert result.crs is not None
    assert result.crs.to_epsg() == NZGD2000


def test_ids_are_unique(tmp_path: Path):
    result = run_sea_polygon(tmp_path, [LAND_POLYGON])
    assert result["id"].is_unique


def test_ids_are_reproducible(tmp_path: Path):
    first = run_sea_polygon(tmp_path, [LAND_POLYGON])
    second = run_sea_polygon(tmp_path, [LAND_POLYGON])
    assert set(first["id"]) == set(second["id"])


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


def test_tile_bounds_cover_world():
    # The single tile at zoom 0 spans the whole Web Mercator extent.
    minx, miny, maxx, maxy = tile_bounds(0, 0, 0)
    assert minx == pytest.approx(-maxx)
    assert miny == pytest.approx(-maxy)
    assert maxx == pytest.approx(20037508.342789244)


def test_tile_quadkey_known_value():
    # Bing Maps reference: tile (3, 5) at zoom 3 has quadkey "213".
    assert tile_quadkey(3, 5, 3) == "213"


def test_tile_range_covers_bounds():
    tiles = list(tile_range((-1_000_000, -1_000_000, 1_000_000, 1_000_000), 3))
    # The area straddles the Web Mercator origin, so it spans the middle tiles.
    assert (3, 3) in tiles
    assert (4, 4) in tiles
