from pathlib import Path

import geopandas as gpd
import pytest
from data_prep.coastline_polygon import NAME_REFERENCE_POINTS, NZGD2000, NZTM2000, run
from shapely.geometry import LineString, box


def _to_nzgd2000(geom):
    return gpd.GeoSeries([geom], crs=NZTM2000).to_crs(NZGD2000).iloc[0]


def run_coastline_polygon(tmp_path: Path, coastline_lines, island_polygons, island_names=None) -> gpd.GeoDataFrame:
    coastline_gdf = gpd.GeoDataFrame({"geometry": coastline_lines}, crs=NZGD2000)
    coastline_path = tmp_path / "coastline.parquet"
    coastline_gdf.to_parquet(coastline_path)

    island_attrs = {"geometry": island_polygons}
    if island_names is not None:
        island_attrs["name"] = island_names
    island_gdf = gpd.GeoDataFrame(island_attrs, crs=NZGD2000)
    island_path = tmp_path / "island.parquet"
    island_gdf.to_parquet(island_path)

    output_path = tmp_path / "output.parquet"
    run(coastline_path, island_path, output_path)
    return gpd.read_parquet(output_path)


# A closed land loop (built in NZTM2000) around a reference point so the
# point-in-polygon naming assigns it that name.
NAMED_NAME, NAMED_POINT = next(iter(NAME_REFERENCE_POINTS.items()))
_land_box = box(NAMED_POINT[0] - 100_000, NAMED_POINT[1] - 100_000, NAMED_POINT[0] + 100_000, NAMED_POINT[1] + 100_000)
LAND_LOOP = _to_nzgd2000(LineString(_land_box.exterior.coords))

# A small offshore island polygon, placed well outside the land loop.
OFFSHORE_ISLAND = _to_nzgd2000(
    box(NAMED_POINT[0] + 250_000, NAMED_POINT[1] - 250_000, NAMED_POINT[0] + 260_000, NAMED_POINT[1] - 240_000)
)


def test_coastline_becomes_polygon(tmp_path: Path):
    result = run_coastline_polygon(tmp_path, [LAND_LOOP], [OFFSHORE_ISLAND])
    assert not result.empty
    assert (result.geometry.geom_type == "Polygon").all()


def test_names_major_land_polygon(tmp_path: Path):
    result = run_coastline_polygon(tmp_path, [LAND_LOOP], [OFFSHORE_ISLAND])
    assert NAMED_NAME in set(result["name"].dropna())


def test_merges_island_polygons(tmp_path: Path):
    result = run_coastline_polygon(tmp_path, [LAND_LOOP], [OFFSHORE_ISLAND])
    # one coastline-derived polygon + one island polygon
    assert len(result) == 2


def test_island_within_land_dropped(tmp_path: Path):
    inland_island = _to_nzgd2000(
        box(NAMED_POINT[0] - 10_000, NAMED_POINT[1] - 10_000, NAMED_POINT[0] + 10_000, NAMED_POINT[1] + 10_000)
    )
    result = run_coastline_polygon(tmp_path, [LAND_LOOP], [inland_island])
    # island falls within the main land, so only the land polygon remains
    assert len(result) == 1


def test_island_name_preserved(tmp_path: Path):
    result = run_coastline_polygon(tmp_path, [LAND_LOOP], [OFFSHORE_ISLAND], island_names=["Some Island"])
    assert "Some Island" in set(result["name"].dropna())


def test_output_is_nzgd2000(tmp_path: Path):
    result = run_coastline_polygon(tmp_path, [LAND_LOOP], [OFFSHORE_ISLAND])
    assert result.crs is not None
    assert result.crs.to_epsg() == NZGD2000


def test_open_coastline_raises(tmp_path: Path):
    open_line = LineString([(175.0, -39.0), (176.0, -39.0)])
    with pytest.raises(ValueError):
        run_coastline_polygon(tmp_path, [open_line], [OFFSHORE_ISLAND])
