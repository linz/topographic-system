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

    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class BridgeLine(BaseTopoModel):
    """Represents bridges crossing roads, railways, walkways or water features."""

    bridge_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    bridge_use2: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    construction_type: Optional[str] = Field(None, description="The type of the feature.", max_length=50)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    status: Optional[str] = Field(None, max_length=25)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class Building(BaseTopoModel):
    """Represents the extent of a building. Captured from aerial imagery."""

    building_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    status: Optional[str] = Field(None, max_length=25)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class BuildingPoint(BaseTopoModel):
    """Represents the location of a building."""

    building_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    orientation: Optional[float] = Field(None)
    status: Optional[str] = Field(None, max_length=25)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class Coastline(BaseTopoModel):
    """Represents the boundary between land and sea."""

    coastline_type: Optional[str] = Field(None, description="The type of the feature.", max_length=50)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    elevation: Optional[int] = Field(None, description="The elevation above mean sea level.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class Contour(BaseTopoModel):
    """Represents elevation lines for terrain representation."""

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


class DescriptiveText(BaseTopoModel):
    """Represents text annotations for labeling or descriptions."""

    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    info_display: Optional[str] = Field(None, max_length=255)
    size: Optional[float] = Field(None)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class FenceLine(BaseTopoModel):
    """"""

    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class FerryCrossing(BaseTopoModel):
    """Indicates ferry links to terminal locations."""

    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class GeographicName(BaseTopoModel):
    """Represents named geographic locations and features."""

    created_at: str = Field(..., description="The date when the feature was created in the database.")
    desc_code: Optional[str] = Field(None, max_length=15)
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    size: Optional[float] = Field(None)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class Island(BaseTopoModel):
    """Represents an island within a water body. Onshore and Offshore."""

    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    group_name: Optional[str] = Field(None, max_length=60)
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class Landcover(BaseTopoModel):
    """Represents surface cover types such asice, moraine, sand. Vegetation layer managed forests etc."""

    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    subtype: Optional[str] = Field(None, description="The type of the feature.", max_length=50)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class LandcoverLine(BaseTopoModel):
    """Represents linear of land cover features such as dredge tailings."""

    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=50)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class LandcoverPoint(BaseTopoModel):
    """Represents land cover feature locations suck as rock outcrops."""

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


class Landuse(BaseTopoModel):
    """Represents areas designated for specific uses such as mines, racetracks."""

    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    landuse_type: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    landuse_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    status: Optional[str] = Field(None, max_length=25)
    substance_extracted: Optional[str] = Field(None, max_length=11)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class LanduseLine(BaseTopoModel):
    """Represents linear of land use features suck as racetracks."""

    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    landuse_type: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    landuse_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class Marine(BaseTopoModel):
    """Represents features in the marine environment such as mangrives, reef, rocks and shoals."""

    composition: Optional[str] = Field(None, max_length=9)
    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


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


class PlacePoint(BaseTopoModel):
    """Represents a named place or locality."""

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
    substance_extracted: Optional[str] = Field(None, max_length=11)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class RailwayLine(BaseTopoModel):
    """Represents railway tracks. Dual tracks may be represented as single entity."""

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


class RailwayStation(BaseTopoModel):
    """Location of a railway station."""

    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class ReliefLine(BaseTopoModel):
    """Represents terrain features such as cliffs."""

    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    relief_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class ReliefPoint(BaseTopoModel):
    """Represents terrain features such as peaks or saddles."""

    created_at: str = Field(..., description="The date when the feature was created in the database.")
    display: Optional[str] = Field(None, max_length=50)
    elevation: Optional[int] = Field(None, description="The elevation above mean sea level.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class ResidentialArea(BaseTopoModel):
    """Represents areas primarily used for housing."""

    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class RoadLine(BaseTopoModel):
    """Represents roads, including highways and streets."""

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
    way_count: Optional[str] = Field(None, max_length=7)
    width_indicator: Optional[str] = Field(None, max_length=5)


class Runway(BaseTopoModel):
    """Represents airport runway areas."""

    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    runway_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    status: Optional[str] = Field(None, max_length=25)
    surface: Optional[str] = Field(None, max_length=10)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class Structure(BaseTopoModel):
    """Represents man-made structures other than buildings (e.g., reservoir, dry_dock, fish_farm, marine_farm, siphon, tank)."""

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


class StructureLine(BaseTopoModel):
    """Represents structural features such as  cableways, ladders, wharf, weirs etc."""

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


class StructurePoint(BaseTopoModel):
    """Represents small structures or structural features such as gates, masts tanks, windmills."""

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
    wreck_of: Optional[str] = Field(None, max_length=6)


class TrackLine(BaseTopoModel):
    """Represents walking tracks, trails, or unsealed paths."""

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


class TransportPoint(BaseTopoModel):
    """Represents transport-related locations (e.g., bus stops, terminals)."""

    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class TrigPoint(BaseTopoModel):
    """Represents a geodetic survey control point."""

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


class TunnelLine(BaseTopoModel):
    """Represents tunnels for roads, railways, or utilities."""

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


class UtilityLine(BaseTopoModel):
    """Represents linear utility infrastructure such as  pipelines, power lines."""

    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    status: Optional[str] = Field(None, max_length=25)
    support_type: Optional[str] = Field(None, description="The type of the feature.", max_length=50)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")
    utility_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)
    visibility: Optional[str] = Field(None, max_length=11)


class UtilityPoint(BaseTopoModel):
    """Represents point utility infrastructure such as poles, towers."""

    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    orientation: Optional[float] = Field(None)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class Vegetation(BaseTopoModel):
    """Represents areas of vegetation landcover such as trees, scrub, vineyards."""

    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    species: Optional[str] = Field(None, max_length=14)
    subtype: Optional[str] = Field(None, description="The type of the feature.", max_length=50)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class VegetationLine(BaseTopoModel):
    """Represents linear vegetation features such as shelter belts."""

    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class VegetationPoint(BaseTopoModel):
    """Represents individual trees or notable groups of trees."""

    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    id: Optional[str] = Field(None, description="The unique identifier for the topographic feature.")
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class Water(BaseTopoModel):
    """Represents water bodies such as  rivers, lakes."""

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
    water_use: Optional[str] = Field(None, description="The use of the feature.", max_length=50)


class WaterLine(BaseTopoModel):
    """Represents water features such as rivers, canals, drains."""

    created_at: str = Field(..., description="The date when the feature was created in the database.")
    feature_type: str = Field(..., description="The specific type of feature being represented (e.g., 'bridge_line', 'building').", max_length=50)
    geometry: dict[str, Any] = Field(..., description="The geometry of the feature.")
    hierarchy: Optional[str] = Field(None, max_length=25)
    id: str = Field(..., description="The unique identifier for the topographic feature.")
    name: Optional[str] = Field(None, description="The name of the feature.", max_length=75)
    t50_fid: Optional[int] = Field(None, description="The unique identifier for the feature in the source database.")
    updated_at: str = Field(..., description="The date when the feature was last updated in the database.")


class WaterPoint(BaseTopoModel):
    """Represents water-related features such as springs."""

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
