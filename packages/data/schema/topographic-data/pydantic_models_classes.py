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
    """Generated model for Airport."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    updated_at: str = Field(...)


class BridgeLine(BaseTopoModel):
    """Generated model for BridgeLine."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    construction_type: Any = Field(...)
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    status: Any = Field(...)
    subtype: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(...)


class Building(BaseTopoModel):
    """Generated model for Building."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    status: Any = Field(...)
    subtype: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    updated_at: str = Field(...)


class BuildingPoint(BaseTopoModel):
    """Generated model for BuildingPoint."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    orientation: Any = Field(...)
    status: Any = Field(...)
    subtype: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    updated_at: str = Field(...)


class Coastline(BaseTopoModel):
    """Generated model for Coastline."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    elevation: Any = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(...)


class Contour(BaseTopoModel):
    """Generated model for Contour."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    definition: Any = Field(...)
    designation: Any = Field(...)
    elevation: Any = Field(...)
    formation: Any = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    updated_at: str = Field(...)


class DescriptiveText(BaseTopoModel):
    """Generated model for DescriptiveText."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    info_display: Any = Field(...)
    metadata: Any = Field(...)
    size: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    updated_at: str = Field(...)


class FenceLine(BaseTopoModel):
    """Generated model for FenceLine."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    updated_at: str = Field(...)


class FerryLine(BaseTopoModel):
    """Generated model for FerryLine."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    subtype: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    updated_at: str = Field(...)


class GeographicName(BaseTopoModel):
    """Generated model for GeographicName."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    desc_code: Any = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    size: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    updated_at: str = Field(...)


class Island(BaseTopoModel):
    """Generated model for Island."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    group_name: Any = Field(...)
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    updated_at: str = Field(...)


class Landcover(BaseTopoModel):
    """Generated model for Landcover."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(...)


class LandcoverLine(BaseTopoModel):
    """Generated model for LandcoverLine."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    updated_at: str = Field(...)


class BBox(BaseTopoModel):
    """GeoParquet 1.1 covering bbox struct."""

    xmax: float = Field(...)
    xmin: float = Field(...)
    ymax: float = Field(...)
    ymin: float = Field(...)


class DataSource(BaseTopoModel):
    """Generated model for DataSource."""

    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")
    source: Any = Field(..., description="Registered source for linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Any = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")


class LandcoverPointCoreTypes(BaseTopoModel):
    """Generated model for LandcoverPointCoreTypes."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    subtype: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(...)


class RockOutcrop(BaseTopoModel):
    """Generated model for RockOutcrop."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    subtype: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    updated_at: str = Field(...)


class Landuse(BaseTopoModel):
    """Generated model for Landuse."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    status: Any = Field(...)
    substance_extracted: Any = Field(...)
    subtype: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(...)


class LanduseLine(BaseTopoModel):
    """Generated model for LanduseLine."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    subtype: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(...)


class LandusePoint(BaseTopoModel):
    """Generated model for LandusePoint."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    status: Any = Field(...)
    substance_extracted: Any = Field(...)
    subtype: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(...)


class Marine(BaseTopoModel):
    """Generated model for Marine."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    composition: Any = Field(...)
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(...)


class MarinePoint(BaseTopoModel):
    """Generated model for MarinePoint."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    updated_at: str = Field(...)


class Nztopo50CartoText(BaseTopoModel):
    """Generated model for Nztopo50CartoText."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    chardistance: Any = Field(...)
    charplace: Any = Field(...)
    colour: Any = Field(...)
    example_point_id: Any = Field(..., description="id (UUID) of the example feature.")
    font: Any = Field(...)
    full_text: Any = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    labelanchor: Any = Field(...)
    offset: Any = Field(...)
    placement: Any = Field(...)
    size: Any = Field(...)
    style: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    text_bend: Any = Field(...)
    text_char_spacing_distance: Any = Field(...)
    text_colour: Any = Field(...)
    text_font: Any = Field(...)
    text_height: Any = Field(...)
    text_orientation: Any = Field(...)
    text_placement: Any = Field(...)
    text_size_type: Any = Field(...)
    text_stretch_length: Any = Field(...)
    text_string: Any = Field(...)
    text_word_spacing_distance: Any = Field(...)
    textanchor: Any = Field(...)


class Nztopo50DmsGrid(BaseTopoModel):
    """Generated model for Nztopo50DmsGrid."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    direction: Any = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    value: float = Field(...)


class Nztopo50Grid(BaseTopoModel):
    """Generated model for Nztopo50Grid."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    direction: Any = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    value: float = Field(...)


class Nztopo50MapSheet(BaseTopoModel):
    """Generated model for Nztopo50MapSheet."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    example_point_id: str = Field(..., description="id (UUID) of the example feature.")
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    origin_x: float = Field(...)
    origin_y: float = Field(...)
    published_at: str = Field(...)
    published_version: str = Field(...)
    sheet_code: str = Field(...)
    sheet_name: str = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    updated_at: str = Field(...)


class PlacePoint(BaseTopoModel):
    """Generated model for PlacePoint."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    composition: Any = Field(...)
    created_at: str = Field(...)
    description: Any = Field(...)
    elevation: Any = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    subtype: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(...)


class RailwayLine(BaseTopoModel):
    """All mainline railway lines are held in the Topo50 data and shown on the Topo50 printed maps. 
Where a railway line is located close to a road, the line held in the data and shown on the printed map 
may be offset from the road sufficient that the two symbols are recognisable at 1:50,000.

Multiple sidings may be held in the data and shown on the printed maps as a single feature"""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    status: Any = Field(...)
    subtype: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    track_type: Any = Field(...)
    type: str = Field(...)
    updated_at: str = Field(...)
    vehicle_type: Any = Field(...)


class RailwayPoint(BaseTopoModel):
    """Generated model for RailwayPoint."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    updated_at: str = Field(...)


class Relief(BaseTopoModel):
    """Generated model for Relief."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    height: Any = Field(...)
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(...)


class ReliefLine(BaseTopoModel):
    """Generated model for ReliefLine."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    height: Any = Field(...)
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    subtype: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(...)


class ReliefPoint(BaseTopoModel):
    """Generated model for ReliefPoint."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    display: Any = Field(...)
    elevation: Any = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    height: Any = Field(...)
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    orientation: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(...)


class ResidentialArea(BaseTopoModel):
    """Generated model for ResidentialArea."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    updated_at: str = Field(...)


class RoadLine(BaseTopoModel):
    """Generated model for RoadLine."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    hierarchy: Any = Field(...)
    highway_number: Any = Field(...)
    id: str = Field(..., description="UUIDv7 of the feature")
    lane_count: Any = Field(...)
    metadata: Any = Field(...)
    name: Any = Field(...)
    road_access: Any = Field(...)
    status: Any = Field(...)
    surface: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    updated_at: str = Field(...)
    way_count: Any = Field(...)
    width_indicator: Any = Field(...)


class Runway(BaseTopoModel):
    """Generated model for Runway."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    status: Any = Field(...)
    subtype: Any = Field(...)
    surface: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    updated_at: str = Field(...)


class Structure(BaseTopoModel):
    """Generated model for Structure."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    lid_type: Any = Field(...)
    metadata: Any = Field(...)
    name: Any = Field(...)
    species: Any = Field(...)
    status: Any = Field(...)
    subtype: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    tank_type: Any = Field(...)
    type: Any = Field(...)
    updated_at: str = Field(...)


class StructureLine(BaseTopoModel):
    """Generated model for StructureLine."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    species: Any = Field(...)
    status: Any = Field(...)
    subtype: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(...)


class StructurePoint(BaseTopoModel):
    """Generated model for StructurePoint."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    height: Any = Field(...)
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    orientation: Any = Field(...)
    status: Any = Field(...)
    subtype: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    tank_type: Any = Field(...)
    type: Any = Field(...)
    updated_at: str = Field(...)


class TrackLine(BaseTopoModel):
    """Generated model for TrackLine."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    status: Any = Field(...)
    subtype: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    track_type: Any = Field(...)
    type: str = Field(...)
    updated_at: str = Field(...)


class TransportPoint(BaseTopoModel):
    """Generated model for TransportPoint."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(...)


class TrigPoint(BaseTopoModel):
    """Generated model for TrigPoint."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    code: Any = Field(...)
    created_at: str = Field(...)
    elevation: Any = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    trig_type: Any = Field(...)
    type: str = Field(...)
    updated_at: str = Field(...)


class TunnelLine(BaseTopoModel):
    """Generated model for TunnelLine."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    status: Any = Field(...)
    subtype: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    tunnel_use: Any = Field(...)
    tunnel_use2: Any = Field(...)
    type: str = Field(...)
    updated_at: str = Field(...)


class UtilityLine(BaseTopoModel):
    """Generated model for UtilityLine."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    status: Any = Field(...)
    subtype: Any = Field(...)
    support_type: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(...)
    visibility: Any = Field(...)


class UtilityPoint(BaseTopoModel):
    """Generated model for UtilityPoint."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    orientation: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(...)


class Vegetation(BaseTopoModel):
    """Generated model for Vegetation."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    species: Any = Field(...)
    subtype: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(...)


class VegetationLine(BaseTopoModel):
    """Generated model for VegetationLine."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    updated_at: str = Field(...)


class VegetationPoint(BaseTopoModel):
    """Generated model for VegetationPoint."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(...)


class Water(BaseTopoModel):
    """Generated model for Water."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    elevation: Any = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    group_name: Any = Field(...)
    height: Any = Field(...)
    hierarchy: Any = Field(...)
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    perennial: Any = Field(...)
    subtype: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    temperature_indicator: Any = Field(...)
    type: Any = Field(...)
    updated_at: str = Field(...)


class WaterLine(BaseTopoModel):
    """Generated model for WaterLine."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    hierarchy: Any = Field(...)
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(...)


class WaterPoint(BaseTopoModel):
    """Generated model for WaterPoint."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    metadata: Any = Field(...)
    name: Any = Field(...)
    orientation: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    temperature_indicator: Any = Field(...)
    type: Any = Field(...)
    updated_at: str = Field(...)
