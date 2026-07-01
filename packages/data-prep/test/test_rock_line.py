from pathlib import Path

import geopandas as gpd
import pytest
from data_prep.rock_line import run
from shapely.geometry import LineString, Polygon


def run_rock_line(tmp_path: Path, coastline_linestring, island_polygon, water_polygon):
    # 10x10 square rock at (0,0) -> (10,10)
    marine_gdf = gpd.GeoDataFrame(
        {"feature_type": ["rock"], "geometry": [Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])]}, crs=2193
    )
    marine_path = tmp_path / "marine.parquet"
    marine_gdf.to_parquet(marine_path)

    coastline_gdf = gpd.GeoDataFrame({"geometry": [coastline_linestring]}, crs=2193)
    coastline_path = tmp_path / "coastline.parquet"
    coastline_gdf.to_parquet(coastline_path)

    island_gdf = gpd.GeoDataFrame({"geometry": [island_polygon]}, crs=2193)
    island_path = tmp_path / "island.parquet"
    island_gdf.to_parquet(island_path)

    water_gdf = gpd.GeoDataFrame({"feature_type": ["lake"], "geometry": [water_polygon]}, crs=2193)
    water_path = tmp_path / "water.parquet"
    water_gdf.to_parquet(water_path)

    output_path = tmp_path / "output.parquet"
    run(marine_path, coastline_path, island_path, water_path, output_path)
    geom = gpd.read_parquet(output_path).to_crs(2193).geometry.iloc[0]
    return geom


def test_clips_coastline(tmp_path: Path):
    # coastline running along rock bottom edge
    coastline_linestring = LineString([(0, 0), (10, 0)])

    # far away from rock, so no clipping
    island_polygon = Polygon([(100, 100), (200, 100), (200, 200), (100, 200)])
    water_polygon = Polygon([(100, 100), (200, 100), (200, 200), (100, 200)])

    geom = run_rock_line(tmp_path, coastline_linestring, island_polygon, water_polygon)
    # 40m perimeter - 10m bottom edge - 1m off each adjacent side.
    assert geom.length == pytest.approx(28, abs=0.1)


def test_clips_island(tmp_path: Path):
    # island overlapping left of rock
    island_polygon = Polygon([(-5, 0), (0, 0), (0, 20), (-5, 20)])

    # far away from rock, so no clipping
    coastline_linestring = LineString([(100, 100), (100, 200)])
    water_polygon = Polygon([(100, 100), (200, 100), (200, 200), (100, 200)])

    geom = run_rock_line(tmp_path, coastline_linestring, island_polygon, water_polygon)
    # 40m perimeter - 10m left edge - 1m off each adjacent side.
    assert geom.length == pytest.approx(28, abs=0.1)


def test_clips_lake(tmp_path: Path):
    # lake overlapping top and left of rock
    water_polygon = Polygon([(0, -5), (15, -5), (15, 10), (0, 10)])

    # far away from rock, so no clipping
    coastline_linestring = LineString([(100, 100), (100, 200)])
    island_polygon = Polygon([(100, 100), (200, 100), (200, 200), (100, 200)])

    geom = run_rock_line(tmp_path, coastline_linestring, island_polygon, water_polygon)
    # 40m perimeter - 10m top edge -10 left edge - 1m off each adjacent side.
    assert geom.length == pytest.approx(18, abs=0.1)
