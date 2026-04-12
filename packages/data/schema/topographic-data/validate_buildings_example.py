"""
Validation script for topographic features using GeoPandas and Pydantic.

Example: Reading Building data from a GeoDataFrame and validating against the Building model.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd
from pydantic import ValidationError

from pydantic_models_classes import Building


def geometry_to_dict(geom: Any) -> dict[str, Any]:
    """Convert a Shapely geometry to GeoJSON format."""
    if geom is None:
        return {"type": "Point", "coordinates": [0, 0]}
    try:
        # Convert geometry to GeoJSON dict
        geojson_str = gpd.GeoSeries([geom]).to_json()
        features = json.loads(geojson_str)
        if features and "features" in features and len(features["features"]) > 0:
            return features["features"][0].get("geometry", {"type": "Point", "coordinates": [0, 0]})
        return {"type": "Point", "coordinates": [0, 0]}
    except Exception:
        # Fallback: return simple point geometry if conversion fails
        return {"type": "Point", "coordinates": [0, 0]}


def validate_building_row(row: pd.Series) -> tuple[bool, str, dict[str, Any] | None]:
    """
    Validate a single row from a DataFrame against the Building model.
    
    Returns:
        (is_valid, message, validated_data)
    """
    try:
        # Prepare the data for validation
        row_data = {
            "topo_id": row.get("topo_id"),
            "t50_fid": row.get("t50_fid"),
            "feature_type": row.get("feature_type"),
            "building_use": row.get("building_use"),
            "status": row.get("status"),
            "name": row.get("name"),
            "capture_method": row.get("capture_method") if pd.notna(row.get("capture_method")) else None,
            "change_type": row.get("change_type"),
            "update_date": row.get("update_date"),
            "create_date": row.get("create_date"),
            "version": row.get("version"),
            # Handle geometry
            "geometry": geometry_to_dict(row.get("geometry")),
        }
        
        # Validate against the model
        validated = Building(**row_data)
        return True, "Valid", validated.model_dump()
        
    except ValidationError as e:
        error_details = []
        for err in e.errors():
            field = err.get("loc", ["unknown"])[0] if err.get("loc") else "unknown"
            msg = err.get("msg", "Unknown error")
            value = row.get(field) if field != "unknown" else "N/A"
            error_details.append(f"{field}={repr(value)}: {msg}")
        errors_str = "; ".join(error_details) if error_details else str(e)
        return False, f"Validation failed: {errors_str}", None
    except Exception as e:
        return False, f"Error parsing row: {str(e)}", None


def validate_buildings_dataframe(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Validate all Building rows in a GeoDataFrame.
    
    Returns:
        DataFrame with validation results (index, is_valid, message, row_id)
    """
    results = []
    
    for idx, (index, row) in enumerate(gdf.iterrows()):
        is_valid, message, _ = validate_building_row(row)
        results.append({
            "row_index": idx,
            "index_label": index,
            "valid": is_valid,
            "message": message,
            "topo_id": row.get("topo_id", "N/A"),
        })
    
    return pd.DataFrame(results)


def print_validation_report(gdf: gpd.GeoDataFrame) -> None:
    """Print a detailed validation report for a GeoDataFrame."""
    print("=" * 80)
    print("BUILDING VALIDATION REPORT")
    print("=" * 80)
    print(f"Total rows: {len(gdf)}")
    print()
    
    results = validate_buildings_dataframe(gdf)
    valid_count = results["valid"].sum()
    invalid_count = len(results) - valid_count
    
    print(f"Valid:   {valid_count} ({100*valid_count/len(results):.1f}%)")
    print(f"Invalid: {invalid_count} ({100*invalid_count/len(results):.1f}%)")
    print()
    
    if invalid_count > 0:
        print("INVALID ROWS:")
        print("-" * 80)
        invalid = results[~results["valid"]]
        for _, record in invalid.iterrows():
            print(f"Row {record['row_index']}: {record['topo_id']}")
            print(f"  → {record['message']}")
            print()


def example_create_sample_geodataframe() -> gpd.GeoDataFrame:
    """Create a sample GeoDataFrame for testing."""
    from shapely.geometry import box
    
    data = {
        "topo_id": ["b-001", "b-002", "b-003", "b-004"],
        "t50_fid": [101, 102, 103, 104],
        "feature_type": ["building", "building", "building", "building"],
        "building_use": ["residential", "commercial", None, "industrial"],
        "status": ["active", "active", "inactive", None],
        "name": ["City Hall", "Central Bank", "Old Station", "Factory A"],
        # Test data set up to FAIL validation
        "capture_method": [
            "this_is_way_too_long_and_exceeds_25_chars",  # Row 0: FAIL - exceeds max_length=25
            None,                                          # Row 1: FAIL - required field is None
            "automated",                                  # Row 2: PASS
            "manual",                                     # Row 3: PASS
        ],
        "change_type": ["new", "updated", "new", "updated"],
        "update_date": ["2024-01-15T10:30:00Z", "2024-01-14T09:00:00Z", "2024-01-10T15:45:00Z", "2024-01-12T11:20:00Z"],
        "create_date": ["2024-01-01T08:00:00Z", "2024-01-05T10:00:00Z", "2024-01-08T12:00:00Z", "2024-01-09T14:00:00Z"],
        "version": [1, 2, 1, 3],
        "geometry": [
            box(174.77, -41.29, 174.78, -41.28),
            box(174.76, -41.30, 174.77, -41.29),
            box(174.75, -41.31, 174.76, -41.30),
            box(174.74, -41.32, 174.75, -41.31),
        ],
    }
    
    gdf = gpd.GeoDataFrame(data, crs="EPSG:4326")
    return gdf


def example_load_from_file(filepath: Path) -> gpd.GeoDataFrame | None:
    """
    Load a GeoDataFrame from a file (GeoJSON, Shapefile, GeoPackage, etc.).
    
    Examples:
        gdf = example_load_from_file(Path("buildings.geojson"))
        gdf = example_load_from_file(Path("buildings.shp"))
        gdf = example_load_from_file(Path("buildings.gpkg"))
    """
    if not filepath.exists():
        print(f"File not found: {filepath}")
        return None
    
    try:
        gdf = gpd.read_file(filepath)
        print(f"Loaded {len(gdf)} features from {filepath.name}")
        return gdf
    except Exception as e:
        print(f"Error loading file: {e}")
        return None


# ============================================================================
# USAGE EXAMPLES
# ============================================================================

if __name__ == "__main__":
    print("BUILDING VALIDATION SCRIPT")
    print()
    
    # Example 1: Create and validate sample data
    print("Example 1: Validating sample GeoDataFrame")
    print("-" * 80)
    gdf = example_create_sample_geodataframe()
    print_validation_report(gdf)
    print()
    
    # Example 2: Validate individual row
    print("Example 2: Validating individual row")
    print("-" * 80)
    first_row = gdf.iloc[0]
    is_valid, message, data = validate_building_row(first_row)
    print(f"Row 0: {first_row['topo_id']}")
    print(f"Valid: {is_valid}")
    print(f"Message: {message}")
    if data:
        print(f"Data:\n{json.dumps(data, indent=2, default=str)}")
    print()
    
    # Example 3: Load from file (uncomment and modify path as needed)
    # print("Example 3: Loading from GeoJSON file")
    # print("-" * 80)
    # gdf = example_load_from_file(Path("buildings.geojson"))
    # if gdf is not None:
    #     print_validation_report(gdf)
