from pathlib import Path
from unittest.mock import MagicMock, patch

import geopandas as gpd  # type: ignore
import pandas as pd  # type: ignore
import pytest  # type: ignore
from shapely.geometry import Point, LineString  # type: ignore

# Import the class to test
import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "nztopo50" / "import" / "core"))
from load_shp_to_themes import Topo50DataLoader  # type: ignore[import]


@pytest.fixture
def sample_layers_excel():
    """Create a sample Excel DataFrame with layer metadata."""
    return pd.DataFrame(
        {
            "object_name": ["Road_Line_obj"],
            "shp_name": ["road_line"],
            "theme": ["transportation"],
            "dataset": ["Transport_Layers"],
            "feature_type": ["road"],
            "layer_name": ["road_line"],
        }
    )


@pytest.fixture
def sample_geodataframe():
    """Create a sample GeoDataFrame for testing."""
    return gpd.GeoDataFrame(
        {
            "use": ["primary"],
            "type": ["asphalt"],
            "width": [10.5],
            "geometry": [LineString([(0, 0), (1, 1)])],
        },
        crs="EPSG:4326",
    )


@pytest.fixture
def temp_count_log(tmp_path):
    """Create a temporary count log file."""
    log_file = tmp_path / "count_log.txt"
    log_file.write_text("")
    return str(log_file)


def test_get_basename_simple_filename():
    """Test get_basename with simple non-underscored filename."""
    file = "/path/to/water.shp"
    shp_name, basename = Topo50DataLoader.get_basename(file)

    assert shp_name == "water"
    assert basename == "water"


def test_get_basename_with_single_underscore():
    """Test get_basename extracts prefix before first underscore."""
    file = "/path/to/road_line.shp"
    shp_name, basename = Topo50DataLoader.get_basename(file)

    assert shp_name == "road_line"
    assert basename == "road"


def test_get_basename_with_multiple_underscores():
    """Test get_basename extracts first two underscore-separated tokens."""
    file = "/path/to/structure_point_2024.shp"
    shp_name, basename = Topo50DataLoader.get_basename(file)

    assert shp_name == "structure_point_2024"
    assert basename == "structure_point"


def test_loader_initialization(temp_count_log, sample_layers_excel):
    """Test Topo50DataLoader initialization and Excel reading."""
    with patch("pandas.read_excel", return_value=sample_layers_excel):
        loader = Topo50DataLoader(
            shapefile_dir="/tmp/shapefiles",
            excel_file="layers.xlsx",
            database="release64",
            count_log=temp_count_log,
        )

    assert loader.output == "release64"
    assert loader.dataset_field == "dataset"
    assert "road_line" in loader.layers_info


def test_loader_layers_info_parsed_correctly(temp_count_log, sample_layers_excel):
    """Test that Excel layers metadata is correctly parsed into layers_info."""
    with patch("pandas.read_excel", return_value=sample_layers_excel):
        loader = Topo50DataLoader(
            shapefile_dir="/tmp/shapefiles",
            excel_file="layers.xlsx",
            database="release64",
            count_log=temp_count_log,
        )

    layer_info = loader.layers_info["road_line"]
    assert layer_info[0] == "Road_Line_obj"  # object_name
    assert layer_info[1] == "transportation"  # theme
    assert layer_info[2] == "road"  # feature_type
    assert layer_info[3] == "road_line"  # layer_name
    assert layer_info[4] == "Transport_Layers"  # dataset


def test_reset_column_names_tunnel_line(tmp_path):
    """Test column renaming for tunnel_line layer."""
    temp_log = tmp_path / "count_log.txt"
    temp_log.write_text("")

    with patch(
        "pandas.read_excel",
        return_value=pd.DataFrame(
            columns=[
                "object_name",
                "shp_name",
                "theme",
                "dataset",
                "feature_type",
                "layer_name",
            ]
        ),
    ):
        loader = Topo50DataLoader(
            shapefile_dir="/tmp",
            excel_file="layers.xlsx",
            database="release64",
            count_log=str(temp_log),
        )

    gdf = gpd.GeoDataFrame(
        {
            "use1": ["vehicle"],
            "use2": ["pedestrian"],
            "type": ["underground"],
            "geometry": [LineString([(0, 0), (1, 1)])],
        }
    )

    result = loader.reset_column_names(gdf, "tunnel_line")
    loader.count_log_file.close()

    assert "tunnel_use" in result.columns
    assert "tunnel_use2" in result.columns
    assert "tunnel_type" in result.columns
    assert "use1" not in result.columns
    assert "use2" not in result.columns


def test_reset_column_names_road_line(tmp_path):
    """Test column renaming for road_line layer with type conversions."""
    temp_log = tmp_path / "count_log.txt"
    temp_log.write_text("")

    with patch(
        "pandas.read_excel",
        return_value=pd.DataFrame(
            columns=[
                "object_name",
                "shp_name",
                "theme",
                "dataset",
                "feature_type",
                "layer_name",
            ]
        ),
    ):
        loader = Topo50DataLoader(
            shapefile_dir="/tmp",
            excel_file="layers.xlsx",
            database="release64",
            count_log=str(temp_log),
        )

    gdf = gpd.GeoDataFrame(
        {
            "hway_num": ["SH1"],
            "num_lanes": [2.0],
            "UFID": [123.5],
            "geometry": [LineString([(0, 0), (1, 1)])],
        }
    )

    result = loader.reset_column_names(gdf, "road_line")
    loader.count_log_file.close()

    assert "highway_number" in result.columns
    assert "lane_count" in result.columns
    assert "t50_fid" in result.columns
    assert result["lane_count"].dtype == "int"
    assert result["t50_fid"].dtype == "int"


def test_reset_column_names_generic_abbreviations(tmp_path):
    """Test generic column abbreviation expansions across all layers."""
    temp_log = tmp_path / "count_log.txt"
    temp_log.write_text("")

    with patch(
        "pandas.read_excel",
        return_value=pd.DataFrame(
            columns=[
                "object_name",
                "shp_name",
                "theme",
                "dataset",
                "feature_type",
                "layer_name",
            ]
        ),
    ):
        loader = Topo50DataLoader(
            shapefile_dir="/tmp",
            excel_file="layers.xlsx",
            database="release64",
            count_log=str(temp_log),
        )

    gdf = gpd.GeoDataFrame(
        {
            "compositn": ["sand"],
            "descriptn": ["main road"],
            "info_disp": ["yes"],
            "veh_type": ["car"],
            "temp": [25],
            "restrictns": ["no entry"],
            "orientatn": ["NW"],
            "geometry": [Point(0, 0)],
        }
    )

    result = loader.reset_column_names(gdf, "generic_layer")
    loader.count_log_file.close()

    assert "composition" in result.columns
    assert "description" in result.columns
    assert "info_display" in result.columns
    assert "vehicle_type" in result.columns
    assert "temperature" in result.columns
    assert "restrictions" in result.columns
    assert "orientation" in result.columns


def test_group_layers(temp_count_log, sample_layers_excel):
    """Test that group_layers collects field definitions by layer."""
    mock_info = {"fields": ["use", "type", "width"]}

    with patch("pandas.read_excel", return_value=sample_layers_excel):
        with patch("load_shp_to_themes.read_info", return_value=mock_info):
            with patch("glob.glob", return_value=["/fake/road_line.shp"]):
                loader = Topo50DataLoader(
                    shapefile_dir="/tmp",
                    excel_file="layers.xlsx",
                    database="release64",
                    count_log=temp_count_log,
                )
                loader.group_layers()

    assert "road_line" in loader.layer_groups
    assert loader.layer_groups["road_line"] == [["use", "type", "width"]]
    loader.count_log_file.close()


def test_compute_common_fields(temp_count_log, sample_layers_excel):
    """Test that compute_common_fields generates union of field sets."""
    with patch("pandas.read_excel", return_value=sample_layers_excel):
        loader = Topo50DataLoader(
            shapefile_dir="/tmp",
            excel_file="layers.xlsx",
            database="release64",
            count_log=temp_count_log,
        )

    # Manually set up layer groups to test compute
    loader.layer_groups = {
        "road_line": [
            ["use", "type", "width"],
            ["use", "surface", "width"],
        ]
    }

    loader.compute_common_fields()

    common = loader.common_fields["road_line"]
    assert "use" in common
    assert "type" in common
    assert "width" in common
    assert "surface" in common


def test_write_dataset_postgis_exception_handling(sample_geodataframe):
    """Test write_dataset catches PostGIS exceptions gracefully."""
    bad_gdf = MagicMock()
    bad_gdf.to_postgis.side_effect = Exception("Database error")

    with patch("load_shp_to_themes.create_engine"):
        # Should not raise, exceptions are caught
        Topo50DataLoader.write_dataset(
            extension="postgis",
            gdf=bad_gdf,
            output_file=None,
            layer_name="road_line",
        )


def test_write_dataset_geojson_driver():
    """Test write_dataset uses correct GeoJSON driver."""
    gdf = MagicMock()
    with patch("load_shp_to_themes.write_dataframe") as mock_write:
        Topo50DataLoader.write_dataset(
            extension="geojson",
            gdf=gdf,
            output_file="/tmp/output.geojson",
            layer_name="road_line",
        )

        # Verify write_dataframe was called with correct driver
        call_args = mock_write.call_args
        assert call_args[1]["driver"] == "GeoJSON"


def test_write_dataset_gpkg_driver():
    """Test write_dataset uses correct GeoPackage driver."""
    gdf = MagicMock()
    with patch("load_shp_to_themes.write_dataframe") as mock_write:
        Topo50DataLoader.write_dataset(
            extension="gpkg",
            gdf=gdf,
            output_file="/tmp/output.gpkg",
            layer_name="road_line",
        )

        call_args = mock_write.call_args
        assert call_args[1]["driver"] == "GPKG"
        assert call_args[1]["layer"] == "road_line"
