"""
Explicit Pydantic model class definitions for topographic features.

Generated from JSON schemas with proper Field constraints and type hints.
"""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class BaseTopoModel(BaseModel):
    """Base class for all topographic feature models."""

    model_config = ConfigDict(extra="forbid")


# ============================================================================
# Base Features (Common across most models)
# ============================================================================


class WaterwayFeatureLine(BaseTopoModel):
    """Represents edges of rapids and waterfalls"""

    topo_id: str = Field(..., description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    feature_type: str = Field(..., max_length=50, description="The specific type of feature being represented (e.g., 'bridge_line', 'building').")
    name: Optional[str] = Field(None, max_length=75, description="The name of the feature.")
    height: Optional[float] = Field(None, description="Height value.")
    capture_method: str = Field(..., max_length=25, description="The method used to capture the data (e.g., 'manual','automated').")
    change_type: str = Field(..., max_length=25, description="The type of change that occurred to the feature (e.g., 'new', 'updated').")
    update_date: str = Field(..., description="The date when the feature was last updated in the database.")
    create_date: str = Field(..., description="The date when the feature was created in the database.")
    version: int = Field(..., description="The version of the feature.")
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")


class DescriptiveText(BaseTopoModel):
    """Represents text annotations for labeling or descriptions."""

    topo_id: str = Field(..., description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    feature_type: str = Field(..., max_length=50, description="The specific type of feature being represented (e.g., 'bridge_line', 'building').")
    info_display: Optional[str] = Field(None, max_length=255, description="Information to display.")
    size: Optional[float] = Field(None, description="Size value.")
    capture_method: str = Field(..., max_length=25, description="The method used to capture the data (e.g., 'manual','automated').")
    change_type: str = Field(..., max_length=25, description="The type of change that occurred to the feature (e.g., 'new', 'updated').")
    update_date: str = Field(..., description="The date when the feature was last updated in the database.")
    create_date: str = Field(..., description="The date when the feature was created in the database.")
    version: int = Field(..., description="The version of the feature.")
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")


class TrackLine(BaseTopoModel):
    """Represents walking tracks, trails, or unsealed paths."""

    topo_id: str = Field(..., description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    feature_type: str = Field(..., max_length=50, description="The specific type of feature being represented (e.g., 'bridge_line', 'building').")
    track_use: Optional[str] = Field(None, max_length=50, description="The use of the feature.")
    track_type: Optional[str] = Field(None, max_length=50, description="The type of the feature.")
    status: Optional[str] = Field(None, max_length=25, description="Status of the track.")
    name: Optional[str] = Field(None, max_length=75, description="The name of the feature.")
    capture_method: str = Field(..., max_length=25, description="The method used to capture the data (e.g., 'manual','automated').")
    change_type: str = Field(..., max_length=25, description="The type of change that occurred to the feature (e.g., 'new', 'updated').")
    update_date: str = Field(..., description="The date when the feature was last updated in the database.")
    create_date: str = Field(..., description="The date when the feature was created in the database.")
    version: int = Field(..., description="The version of the feature.")
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")


class Building(BaseTopoModel):
    """Represents the extent of a building. Captured from aerial imagery."""

    topo_id: str = Field(..., description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    feature_type: str = Field(..., max_length=50, description="The specific type of feature being represented (e.g., 'bridge_line', 'building').")
    building_use: Optional[str] = Field(None, max_length=50, description="The use of the feature.")
    status: Optional[str] = Field(None, max_length=25, description="Status of the building.")
    name: Optional[str] = Field(None, max_length=75, description="The name of the feature.")
    capture_method: str = Field(..., max_length=25, description="The method used to capture the data (e.g., 'manual','automated').")
    change_type: str = Field(..., max_length=25, description="The type of change that occurred to the feature (e.g., 'new', 'updated').")
    update_date: str = Field(..., description="The date when the feature was last updated in the database.")
    create_date: str = Field(..., description="The date when the feature was created in the database.")
    version: int = Field(..., description="The version of the feature.")
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")


class BuildingPoint(BaseTopoModel):
    """Represents the location of a building."""

    topo_id: str = Field(..., description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    feature_type: str = Field(..., max_length=50, description="The specific type of feature being represented (e.g., 'bridge_line', 'building').")
    building_use: Optional[str] = Field(None, max_length=50, description="The use of the feature.")
    status: Optional[str] = Field(None, max_length=25, description="Status of the building.")
    name: Optional[str] = Field(None, max_length=75, description="The name of the feature.")
    orientation: Optional[float] = Field(None, description="Orientation value.")
    capture_method: str = Field(..., max_length=25, description="The method used to capture the data (e.g., 'manual','automated').")
    change_type: str = Field(..., max_length=25, description="The type of change that occurred to the feature (e.g., 'new', 'updated').")
    update_date: str = Field(..., description="The date when the feature was last updated in the database.")
    create_date: str = Field(..., description="The date when the feature was created in the database.")
    version: int = Field(..., description="The version of the feature.")
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")


class RoadLine(BaseTopoModel):
    """Represents roads, including highways and streets."""

    topo_id: str = Field(..., description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    feature_type: str = Field(..., max_length=50, description="The specific type of feature being represented (e.g., 'bridge_line', 'building').")
    hierarchy: Optional[str] = Field(None, max_length=50, description="Road hierarchy level.")
    status: Optional[str] = Field(None, max_length=25, description="Status of the road.")
    name: Optional[str] = Field(None, max_length=75, description="The name of the feature.")
    highway_number: Optional[str] = Field(None, max_length=20, description="Highway number.")
    lane_count: Optional[int] = Field(None, description="Number of lanes.")
    surface: Optional[str] = Field(None, max_length=10, description="Surface type.")
    way_count: Optional[str] = Field(None, max_length=7, description="Way count.")
    road_access: Optional[str] = Field(None, max_length=5, description="Road access type.")
    name_id: Optional[int] = Field(None, description="Name identifier.")
    rna_sufi: Optional[int] = Field(None, description="RNA SUFI code.")
    capture_method: str = Field(..., max_length=25, description="The method used to capture the data (e.g., 'manual','automated').")
    change_type: str = Field(..., max_length=25, description="The type of change that occurred to the feature (e.g., 'new', 'updated').")
    update_date: str = Field(..., description="The date when the feature was last updated in the database.")
    create_date: str = Field(..., description="The date when the feature was created in the database.")
    version: int = Field(..., description="The version of the feature.")
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")


class WaterLine(BaseTopoModel):
    """Represents water features such as rivers, canals, drains."""

    topo_id: str = Field(..., description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    feature_type: str = Field(..., max_length=50, description="The specific type of feature being represented (e.g., 'bridge_line', 'building').")
    hierarchy: Optional[str] = Field(None, max_length=25, description="Water hierarchy level.")
    status: Optional[str] = Field(None, max_length=25, description="Status of the water line.")
    name: Optional[str] = Field(None, max_length=75, description="The name of the feature.")
    height: Optional[float] = Field(None, description="Height value.")
    capture_method: str = Field(..., max_length=25, description="The method used to capture the data (e.g., 'manual','automated').")
    change_type: str = Field(..., max_length=25, description="The type of change that occurred to the feature (e.g., 'new', 'updated').")
    update_date: str = Field(..., description="The date when the feature was last updated in the database.")
    create_date: str = Field(..., description="The date when the feature was created in the database.")
    version: int = Field(..., description="The version of the feature.")
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")


class Contour(BaseTopoModel):
    """Represents elevation lines for terrain representation."""

    topo_id: str = Field(..., description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    feature_type: str = Field(..., description="The specific type of feature being represented.")
    elevation: Optional[int] = Field(None, description="Elevation value.")
    definition: Optional[str] = Field(None, description="Definition of the contour.")
    designation: Optional[str] = Field(None, description="Designation of the contour.")
    formation: Optional[str] = Field(None, description="Formation of the contour.")
    capture_method: str = Field(..., max_length=25, description="The method used to capture the data (e.g., 'manual','automated').")
    change_type: str = Field(..., max_length=25, description="The type of change that occurred to the feature (e.g., 'new', 'updated').")
    update_date: str = Field(..., description="The date when the feature was last updated in the database.")
    create_date: str = Field(..., description="The date when the feature was created in the database.")
    version: int = Field(..., description="The version of the feature.")
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")


class Water(BaseTopoModel):
    """Represents water bodies such as rivers, lakes."""

    topo_id: str = Field(..., description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    feature_type: str = Field(..., max_length=50, description="The specific type of feature being represented (e.g., 'bridge_line', 'building').")
    water_use: Optional[str] = Field(None, max_length=50, description="The use of the feature.")
    hierarchy: Optional[str] = Field(None, max_length=25, description="Water hierarchy level.")
    name: Optional[str] = Field(None, max_length=75, description="The name of the feature.")
    group_name: Optional[str] = Field(None, max_length=60, description="Group name.")
    height: Optional[float] = Field(None, description="Height value.")
    perennial: Optional[str] = Field(None, max_length=8, description="Perennial indicator.")
    temperature: Optional[str] = Field(None, max_length=3, description="Temperature indicator.")
    capture_method: str = Field(..., max_length=25, description="The method used to capture the data (e.g., 'manual','automated').")
    change_type: str = Field(..., max_length=25, description="The type of change that occurred to the feature (e.g., 'new', 'updated').")
    update_date: str = Field(..., description="The date when the feature was last updated in the database.")
    create_date: str = Field(..., description="The date when the feature was created in the database.")
    version: int = Field(..., description="The version of the feature.")
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")


class BridgeLine(BaseTopoModel):
    """Represents bridges crossing roads, railways, walkways or water features."""

    topo_id: str = Field(..., description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    feature_type: str = Field(..., max_length=50, description="The specific type of feature being represented (e.g., 'bridge_line', 'building').")
    bridge_use: Optional[str] = Field(None, max_length=50, description="The use of the feature.")
    bridge_use2: Optional[str] = Field(None, max_length=50, description="Secondary use of the feature.")
    construction_type: Optional[str] = Field(None, max_length=50, description="The type of the feature.")
    status: Optional[str] = Field(None, max_length=25, description="Status of the bridge.")
    name: Optional[str] = Field(None, max_length=75, description="The name of the feature.")
    capture_method: str = Field(..., max_length=25, description="The method used to capture the data (e.g., 'manual','automated').")
    change_type: str = Field(..., max_length=25, description="The type of change that occurred to the feature (e.g., 'new', 'updated').")
    update_date: str = Field(..., description="The date when the feature was last updated in the database.")
    create_date: str = Field(..., description="The date when the feature was created in the database.")
    version: int = Field(..., description="The version of the feature.")
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")


class RailwayLine(BaseTopoModel):
    """Represents railway tracks. Dual tracks may be represented as single entity."""

    topo_id: str = Field(..., description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    feature_type: str = Field(..., max_length=50, description="The specific type of feature being represented (e.g., 'bridge_line', 'building').")
    railway_use: Optional[str] = Field(None, max_length=50, description="The use of the feature.")
    track_type: Optional[str] = Field(None, max_length=50, description="The type of the feature.")
    vehicle_type: Optional[str] = Field(None, max_length=50, description="The type of vehicle.")
    status: Optional[str] = Field(None, max_length=25, description="Status of the railway.")
    name: Optional[str] = Field(None, max_length=75, description="The name of the feature.")
    route: Optional[str] = Field(None, max_length=30, description="Route information.")
    route2: Optional[str] = Field(None, max_length=30, description="Route information 2.")
    route3: Optional[str] = Field(None, max_length=30, description="Route information 3.")
    capture_method: str = Field(..., max_length=25, description="The method used to capture the data (e.g., 'manual','automated').")
    change_type: str = Field(..., max_length=25, description="The type of change that occurred to the feature (e.g., 'new', 'updated').")
    update_date: str = Field(..., description="The date when the feature was last updated in the database.")
    create_date: str = Field(..., description="The date when the feature was created in the database.")
    version: int = Field(..., description="The version of the feature.")
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")


class Vegetation(BaseTopoModel):
    """Represents areas of vegetation landcover such as trees, scrub, vineyards."""

    topo_id: str = Field(..., description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    feature_type: str = Field(..., max_length=50, description="The specific type of feature being represented (e.g., 'bridge_line', 'building').")
    sub_type: Optional[str] = Field(None, max_length=50, description="The type of the feature.")
    species: Optional[str] = Field(None, max_length=14, description="Species type.")
    name: Optional[str] = Field(None, max_length=50, description="The name of the feature.")
    capture_method: str = Field(..., max_length=25, description="The method used to capture the data (e.g., 'manual','automated').")
    change_type: str = Field(..., max_length=25, description="The type of change that occurred to the feature (e.g., 'new', 'updated').")
    update_date: str = Field(..., description="The date when the feature was last updated in the database.")
    create_date: str = Field(..., description="The date when the feature was created in the database.")
    version: int = Field(..., description="The version of the feature.")
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")


class Coastline(BaseTopoModel):
    """Represents the boundary between land and sea."""

    topo_id: str = Field(..., description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    feature_type: str = Field(..., max_length=50, description="The specific type of feature being represented (e.g., 'bridge_line', 'building').")
    coastline_type: Optional[str] = Field(None, max_length=50, description="The type of the feature.")
    elevation: Optional[int] = Field(None, description="Elevation value.")
    capture_method: str = Field(..., max_length=25, description="The method used to capture the data (e.g., 'manual','automated').")
    change_type: str = Field(..., max_length=25, description="The type of change that occurred to the feature (e.g., 'new', 'updated').")
    update_date: str = Field(..., description="The date when the feature was last updated in the database.")
    create_date: str = Field(..., description="The date when the feature was created in the database.")
    version: int = Field(..., description="The version of the feature.")
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")


__all__ = [
    "BaseTopoModel",
    "WaterwayFeatureLine",
    "DescriptiveText",
    "TrackLine",
    "Building",
    "BuildingPoint",
    "RoadLine",
    "WaterLine",
    "Contour",
    "Water",
    "BridgeLine",
    "RailwayLine",
    "Vegetation",
    "Coastline",
]
