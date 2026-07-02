from pathlib import Path

import geopandas as gpd
import pytest
from data_prep.rock_line import NZGD2000, run
from shapely.geometry import LineString, Polygon

# Rock and shorelines in NZGD2000
ROCK = Polygon([(174, -41), (175, -41), (175, -40), (174, -40)])
ROCK_PERIMETER = ROCK.exterior.length

# NEAR: coincides with the rock's bottom edge (lat -41), so it clips that edge.
NEAR_LINE = LineString([(174, -41), (175, -41)])
NEAR_POLYGON = Polygon([(174, -42), (175, -42), (175, -41), (174, -41)])

# FAR: well clear of the rock, so it never clips it.
FAR_LINE = LineString([(170, -45), (171, -45)])
FAR_POLYGON = Polygon([(170, -45), (171, -45), (171, -44), (170, -44)])


def run_rock_line(tmp_path: Path, coastline_line, island_polygon, water_polygon):
    marine_gdf = gpd.GeoDataFrame({"type": ["rock"], "geometry": [ROCK]}, crs=NZGD2000)
    marine_path = tmp_path / "marine.parquet"
    marine_gdf.to_parquet(marine_path)

    coastline_gdf = gpd.GeoDataFrame({"geometry": [coastline_line]}, crs=NZGD2000)
    coastline_path = tmp_path / "coastline.parquet"
    coastline_gdf.to_parquet(coastline_path)

    island_gdf = gpd.GeoDataFrame({"geometry": [island_polygon]}, crs=NZGD2000)
    island_path = tmp_path / "island.parquet"
    island_gdf.to_parquet(island_path)

    water_gdf = gpd.GeoDataFrame({"type": ["lake"], "geometry": [water_polygon]}, crs=NZGD2000)
    water_path = tmp_path / "water.parquet"
    water_gdf.to_parquet(water_path)

    output_path = tmp_path / "output.parquet"
    run(marine_path, coastline_path, island_path, water_path, output_path)
    geom = gpd.read_parquet(output_path).geometry.iloc[0]
    return geom


def test_no_clip_when_far(tmp_path: Path):
    # nothing coincides with the rock, so the full outline survives
    geom = run_rock_line(tmp_path, FAR_LINE, FAR_POLYGON, FAR_POLYGON)
    assert geom.length == pytest.approx(ROCK_PERIMETER)


def test_clips_coastline(tmp_path: Path):
    geom = run_rock_line(tmp_path, NEAR_LINE, FAR_POLYGON, FAR_POLYGON)
    assert geom.length < ROCK_PERIMETER


def test_clips_island(tmp_path: Path):
    geom = run_rock_line(tmp_path, FAR_LINE, NEAR_POLYGON, FAR_POLYGON)
    assert geom.length < ROCK_PERIMETER


def test_clips_lake(tmp_path: Path):
    geom = run_rock_line(tmp_path, FAR_LINE, FAR_POLYGON, NEAR_POLYGON)
    assert geom.length < ROCK_PERIMETER
