from pathlib import Path

import geopandas as gpd
import pytest
from data_prep.coastline_polygon import NAMES_BY_AREA_RANK, NZGD2000, run
from shapely.geometry import LineString, Polygon


def _closed_loop(coords) -> LineString:
    return LineString(coords + [coords[0]])


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


# A single closed coastline loop; as the only/largest land polygon it takes the
# first name in NAMES_BY_AREA_RANK.
LARGEST_NAME = NAMES_BY_AREA_RANK[0]
LAND_LOOP = _closed_loop(
    [
        (174.0, -40.0),
        (176.0, -40.0),
        (176.0, -38.0),
        (174.0, -38.0),
    ]
)

# A small offshore island polygon.
OFFSHORE_ISLAND = Polygon([(178.0, -37.0), (178.1, -37.0), (178.1, -36.9), (178.0, -36.9)])


def test_coastline_becomes_polygon(tmp_path: Path):
    result = run_coastline_polygon(tmp_path, [LAND_LOOP], [OFFSHORE_ISLAND])
    assert not result.empty
    assert (result.geometry.geom_type == "Polygon").all()


def test_names_major_land_polygon(tmp_path: Path):
    result = run_coastline_polygon(tmp_path, [LAND_LOOP], [OFFSHORE_ISLAND])
    assert LARGEST_NAME in set(result["name"].dropna())


def test_merges_island_polygons(tmp_path: Path):
    result = run_coastline_polygon(tmp_path, [LAND_LOOP], [OFFSHORE_ISLAND])
    # one coastline-derived polygon + one island polygon
    assert len(result) == 2


def test_island_name_preserved(tmp_path: Path):
    result = run_coastline_polygon(tmp_path, [LAND_LOOP], [OFFSHORE_ISLAND], island_names=["Some Island"])
    assert "Some Island" in set(result["name"].dropna())


def test_output_is_nzgd2000(tmp_path: Path):
    result = run_coastline_polygon(tmp_path, [LAND_LOOP], [OFFSHORE_ISLAND])
    assert result.crs.to_epsg() == NZGD2000


def test_open_coastline_raises(tmp_path: Path):
    open_line = LineString([(175.0, -39.0), (176.0, -39.0)])
    with pytest.raises(ValueError):
        run_coastline_polygon(tmp_path, [open_line], [OFFSHORE_ISLAND])
