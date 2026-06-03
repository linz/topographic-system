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



class Airport(BaseTopoModel):
    """Represents the boundary of an airport facility."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class BridgeLine(BaseTopoModel):
    """Represents bridges crossing roads, railways, walkways or water features."""

    bridge_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    bridge_use2: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    construction_type: Optional[str] = Field(None, description="The type of the feature.", max_length=50)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    status: Optional[str] = Field(None, max_length=25)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class Building(BaseTopoModel):
    """Represents the extent of a building. Captured from aerial imagery."""

    building_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    status: Optional[str] = Field(None, max_length=25)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class BuildingPoint(BaseTopoModel):
    """Represents the location of a building."""

    building_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    orientation: Optional[float] = Field(None)
    status: Optional[str] = Field(None, max_length=25)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class Coastline(BaseTopoModel):
    """Represents the boundary between land and sea."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    coastline_type: Optional[str] = Field(None, description="The type of the feature.", max_length=50)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    elevation: Optional[int] = Field(None, description="The elevation above mean sea level.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class Contour(BaseTopoModel):
    """Represents elevation lines for terrain representation."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    definition: Optional[str] = Field(None)
    designation: Optional[str] = Field(None)
    elevation: Optional[int] = Field(None, description="The elevation above mean sea level.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').")
    formation: Optional[str] = Field(None)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class DescriptiveText(BaseTopoModel):
    """Represents text annotations for labeling or descriptions."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    info_display: Optional[str] = Field(None, max_length=255)
    size: Optional[float] = Field(None)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class FenceLine(BaseTopoModel):
    """"""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class FerryCrossing(BaseTopoModel):
    """Indicates ferry links to terminal locations."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class GeographicName(BaseTopoModel):
    """Represents named geographic locations and features."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    desc_code: Optional[str] = Field(None, max_length=15)
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    size: Optional[float] = Field(None)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class Island(BaseTopoModel):
    """Represents an island within a water body. Onshore and Offshore."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    group_name: Optional[str] = Field(None, max_length=60)
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class Landcover(BaseTopoModel):
    """Represents surface cover types such asice, moraine, sand. Vegetation layer managed forests etc."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    sub_type: Optional[str] = Field(None, description="The type of the feature.", max_length=50)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class LandcoverLine(BaseTopoModel):
    """Represents linear of land cover features such as dredge tailings."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=50)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class LandcoverPoint(BaseTopoModel):
    """Represents land cover feature locations suck as rock outcrops."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    display: Optional[str] = Field(None, max_length=50)
    elevation: Optional[int] = Field(None, description="The elevation above mean sea level.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    orientation: Optional[float] = Field(None)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class Landuse(BaseTopoModel):
    """Represents areas designated for specific uses such as mines, racetracks."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    landuse_type: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    landuse_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    status: Optional[str] = Field(None, max_length=25)
    substance: Optional[str] = Field(None, max_length=11)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class LanduseLine(BaseTopoModel):
    """Represents linear of land use features suck as racetracks."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    landuse_type: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    landuse_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class Marine(BaseTopoModel):
    """Represents features in the marine environment such as mangrives, reef, rocks and shoals."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    composition: Optional[str] = Field(None, max_length=9)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class NzTopo50MapSheet(BaseTopoModel):
    """Represents the LINZ Topographic 1:50,000 map sheets."""

    edition: str = Field(..., max_length=30)
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    revised: Optional[str] = Field(None, max_length=60)
    sheet_code: str = Field(..., max_length=21)
    sheet_name: str = Field(..., max_length=50)
    t50_fid: int = Field(..., description="The unique identifier for the feature in the source database.")


class PhysicalInfrastructureLine(BaseTopoModel):
    """Represents linear infrastructure such as  pipelines, power lines."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    infrastructure_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    status: Optional[str] = Field(None, max_length=25)
    support_type: Optional[str] = Field(None, description="The type of the feature.", max_length=50)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")
    visibility: Optional[str] = Field(None, max_length=11)


class PhysicalInfrastructurePoint(BaseTopoModel):
    """Represents point infrastructure such as poles, towers."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    orientation: Optional[float] = Field(None)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class PlacePoint(BaseTopoModel):
    """Represents a named place or locality."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    composition: Optional[str] = Field(None, max_length=9)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    description: Optional[str] = Field(None, max_length=80)
    elevation: Optional[int] = Field(None, description="The elevation above mean sea level.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    orientation: Optional[float] = Field(None)
    place_type: Optional[str] = Field(None, description="The type of the feature.", max_length=11)
    status: Optional[str] = Field(None, max_length=25)
    substance: Optional[str] = Field(None, max_length=11)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class RailwayLine(BaseTopoModel):
    """Represents railway tracks. Dual tracks may be represented as single entity."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    railway_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    route: Optional[str] = Field(None, max_length=30)
    route2: Optional[str] = Field(None, max_length=30)
    route3: Optional[str] = Field(None, max_length=30)
    status: Optional[str] = Field(None, max_length=25)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    track_type: Optional[str] = Field(None, description="The type of the feature.", max_length=50)
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    vehicle_type: Optional[str] = Field(None, description="The type of the feature.", max_length=50)
    version: int = Field(..., description="The version of the feature.")


class RailwayStation(BaseTopoModel):
    """Location of a railway station."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class ReliefLine(BaseTopoModel):
    """Represents terrain features such as cliffs."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    relief_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class ReliefPoint(BaseTopoModel):
    """Represents terrain features such as peaks or saddles."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    display: Optional[str] = Field(None, max_length=50)
    elevation: Optional[int] = Field(None, description="The elevation above mean sea level.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class ResidentialArea(BaseTopoModel):
    """Represents areas primarily used for housing."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class RoadLine(BaseTopoModel):
    """Represents roads, including highways and streets."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    hierarchy: Optional[str] = Field(None, max_length=50)
    highway_number: Optional[str] = Field(None, max_length=20)
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    lane_count: Optional[int] = Field(None)
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    name_id: Optional[int] = Field(None)
    rna_sufi: Optional[int] = Field(None)
    road_access: Optional[str] = Field(None, max_length=5)
    status: Optional[str] = Field(None, max_length=25)
    surface: Optional[str] = Field(None, max_length=10)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")
    way_count: Optional[str] = Field(None, max_length=7)
    width_indicator: Optional[str] = Field(None, max_length=5)


class Runway(BaseTopoModel):
    """Represents airport runway areas."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    runway_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    status: Optional[str] = Field(None, max_length=25)
    surface: Optional[str] = Field(None, max_length=10)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class Structure(BaseTopoModel):
    """Represents man-made structures other than buildings (e.g., reservoir, dry_dock, fish_farm, marine_farm, siphon, tank)."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    lid_type: Optional[str] = Field(None, description="The type of the feature.", max_length=50)
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=50)
    species: Optional[str] = Field(None, max_length=14)
    status: Optional[str] = Field(None, max_length=25)
    stored_item: Optional[str] = Field(None, max_length=5)
    structure_type: Optional[str] = Field(None, description="The type of the feature.", max_length=50)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class StructureLine(BaseTopoModel):
    """Represents structural features such as  cableways, ladders, wharf, weirs etc."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    material: Optional[str] = Field(None, max_length=5)
    material_conveyed: Optional[str] = Field(None, max_length=4)
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    restrictions: Optional[str] = Field(None, max_length=6)
    species: Optional[str] = Field(None, max_length=14)
    status: Optional[str] = Field(None, max_length=25)
    structure_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class StructurePoint(BaseTopoModel):
    """Represents small structures or structural features such as gates, masts tanks, windmills."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    height: Optional[float] = Field(None, description="The height of the feature in metres")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    location: Optional[str] = Field(None, max_length=5)
    material: Optional[str] = Field(None, max_length=8)
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    orientation: Optional[float] = Field(None)
    restrictions: Optional[str] = Field(None, max_length=6)
    status: Optional[str] = Field(None, max_length=25)
    stored_item: Optional[str] = Field(None, max_length=5)
    structure_type: Optional[str] = Field(None, description="The type of the feature.", max_length=50)
    structure_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")
    wreck_of: Optional[str] = Field(None, max_length=6)


class TrackLine(BaseTopoModel):
    """Represents walking tracks, trails, or unsealed paths."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    status: Optional[str] = Field(None, max_length=25)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    track_type: Optional[str] = Field(None, description="The type of the feature.", max_length=50)
    track_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class TransportPoint(BaseTopoModel):
    """Represents transport-related locations (e.g., bus stops, terminals)."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class TreePoint(BaseTopoModel):
    """Represents individual trees or notable groups of trees."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: Optional[str] = Field(None, description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class TrigPoint(BaseTopoModel):
    """Represents a geodetic survey control point."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    code: Optional[str] = Field(None, max_length=20)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    elevation: Optional[int] = Field(None, description="The elevation above mean sea level.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    trig_type: Optional[str] = Field(None, description="The type of the feature.", max_length=50)
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class TunnelLine(BaseTopoModel):
    """Represents tunnels for roads, railways, or utilities."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    status: Optional[str] = Field(None, max_length=25)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    tunnel_type: Optional[str] = Field(None, description="The type of the feature.", max_length=50)
    tunnel_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    tunnel_use2: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class Vegetation(BaseTopoModel):
    """Represents areas of vegetation landcover such as trees, scrub, vineyards."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    species: Optional[str] = Field(None, max_length=14)
    sub_type: Optional[str] = Field(None, description="The type of the feature.", max_length=50)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class VegetationLine(BaseTopoModel):
    """Represents linear vegetation features such as shelter belts."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class Water(BaseTopoModel):
    """Represents water bodies such as  rivers, lakes."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    elevation: Optional[int] = Field(None, description="The elevation above mean sea level.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    group_name: Optional[str] = Field(None, max_length=60)
    height: Optional[float] = Field(None, description="The height of the feature in metres")
    hierarchy: Optional[str] = Field(None, max_length=25)
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    nzgb_feat_id: Optional[float] = Field(None)
    perennial: Optional[str] = Field(None, max_length=8)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    temperature: Optional[str] = Field(None, max_length=3)
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")
    water_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)


class WaterLine(BaseTopoModel):
    """Represents water features such as rivers, canals, drains."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    height: Optional[float] = Field(None, description="The height of the feature in metres")
    hierarchy: Optional[str] = Field(None, max_length=25)
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    status: Optional[str] = Field(None, max_length=25)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")


class WaterPoint(BaseTopoModel):
    """Represents water-related features such as springs."""

    capture_method: str = Field(..., description="The method used to capture the data (e.g., 'manual','automated').", max_length=25)
    change_type: str = Field(..., description="The type of change that occurred to the feature (e.g., 'new', 'updated').", max_length=25)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    height: Optional[float] = Field(None, description="The height of the feature in metres")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    orientation: Optional[float] = Field(None)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    temperature_indicator: Optional[str] = Field(None, max_length=4)
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    version: int = Field(..., description="The version of the feature.")
