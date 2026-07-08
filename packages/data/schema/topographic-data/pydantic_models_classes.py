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

    bbox: Any = Field(...)
    created_at: Any = Field(..., description="Default at write time: today.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    name: Any = Field(...)
    t50_fid: Optional[Any] = Field(None)
    type: str = Field(...)
    updated_at: Any = Field(...)


class AuditFields(BaseTopoModel):
    """Capture / change-tracking columns shared by most features."""

    created_at: Any = Field(..., description="Default at write time: today.")
    updated_at: Any = Field(...)


class BridgeLine(BaseTopoModel):
    """Generated model for BridgeLine."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    construction_type: Any = Field(...)
    created_at: str = Field(..., description="ISO Datetime of when the feature was created")
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    name: Any = Field(...)
    status: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    updated_at: str = Field(..., description="ISO Datetime of when the feature was last updated")
    use1: Any = Field(...)
    use2: Any = Field(...)


class Building(BaseTopoModel):
    """Generated model for Building."""

    bbox: Optional[Any] = Field(None)
    building_use: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    name: Optional[Any] = Field(None)
    status: Optional[Any] = Field(None)
    t50_fid: Optional[Any] = Field(None)
    type: str = Field(...)
    updated_at: Any = Field(...)


class BuildingPoint(BaseTopoModel):
    """Generated model for BuildingPoint."""

    bbox: Optional[Any] = Field(None)
    building_use: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    name: Optional[Any] = Field(None)
    orientation: Optional[Any] = Field(None)
    status: Optional[Any] = Field(None)
    t50_fid: Optional[Any] = Field(None)
    type: str = Field(...)
    updated_at: Any = Field(...)


class Coastline(BaseTopoModel):
    """Generated model for Coastline."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(..., description="ISO Datetime of when the feature was created")
    elevation: Any = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(..., description="ISO Datetime of when the feature was last updated")


class Contour(BaseTopoModel):
    """Generated model for Contour."""

    bbox: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    definition: Optional[Any] = Field(None)
    designation: Optional[Any] = Field(None)
    elevation: Optional[Any] = Field(None)
    formation: Optional[Any] = Field(None)
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    t50_fid: Optional[Any] = Field(None)
    type: str = Field(...)
    updated_at: Any = Field(...)


class DescriptiveText(BaseTopoModel):
    """Generated model for DescriptiveText."""

    bbox: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    info_display: Optional[Any] = Field(None)
    nzgb_id: Optional[Any] = Field(None)
    size: Optional[Any] = Field(None)
    t50_fid: Optional[Any] = Field(None)
    type: str = Field(...)
    updated_at: Any = Field(...)


class FenceLine(BaseTopoModel):
    """Generated model for FenceLine."""

    bbox: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    t50_fid: Optional[Any] = Field(None)
    type: str = Field(...)
    updated_at: Any = Field(...)


class FerryCrossing(BaseTopoModel):
    """Generated model for FerryCrossing."""

    bbox: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    t50_fid: Optional[Any] = Field(None)
    type: str = Field(...)
    updated_at: Any = Field(...)


class FerryLine(BaseTopoModel):
    """Generated model for FerryLine."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(..., description="ISO Datetime of when the feature was created")
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    updated_at: str = Field(..., description="ISO Datetime of when the feature was last updated")


class GeographicName(BaseTopoModel):
    """Generated model for GeographicName."""

    bbox: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    desc_code: Optional[Any] = Field(None)
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    name: Optional[Any] = Field(None)
    size: Optional[Any] = Field(None)
    t50_fid: Optional[Any] = Field(None)
    type: str = Field(...)
    updated_at: Any = Field(...)


class Island(BaseTopoModel):
    """Generated model for Island."""

    bbox: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    group_name: Optional[Any] = Field(None)
    id: str = Field(..., description="UUID for the feature.")
    name: Optional[Any] = Field(None)
    t50_fid: Optional[Any] = Field(None)
    type: str = Field(...)
    updated_at: Any = Field(...)


class Landcover(BaseTopoModel):
    """Generated model for Landcover."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(..., description="ISO Datetime of when the feature was created")
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    name: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(..., description="ISO Datetime of when the feature was last updated")


class LandcoverLine(BaseTopoModel):
    """Generated model for LandcoverLine."""

    bbox: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    t50_fid: Optional[Any] = Field(None)
    type: str = Field(...)
    updated_at: Any = Field(...)


class LandcoverPoint(BaseTopoModel):
    """Generated model for LandcoverPoint."""

    bbox: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    display: Optional[Any] = Field(None)
    elevation: Optional[Any] = Field(None)
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    name: Optional[Any] = Field(None)
    orientation: Optional[Any] = Field(None)
    t50_fid: Optional[Any] = Field(None)
    type: Any = Field(...)
    updated_at: Any = Field(...)


class Landuse(BaseTopoModel):
    """Generated model for Landuse."""

    bbox: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    landuse_use: Optional[Any] = Field(None)
    name: Optional[Any] = Field(None)
    status: Optional[Any] = Field(None)
    substance_extracted: Optional[Any] = Field(None)
    subtype: Optional[Any] = Field(None)
    t50_fid: Optional[Any] = Field(None)
    type: Any = Field(...)
    updated_at: Any = Field(...)


class LanduseLine(BaseTopoModel):
    """Generated model for LanduseLine."""

    bbox: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    landuse_use: Optional[Any] = Field(None)
    name: Optional[Any] = Field(None)
    subtype: Optional[Any] = Field(None)
    t50_fid: Optional[Any] = Field(None)
    type: str = Field(...)
    updated_at: Any = Field(...)


class Marine(BaseTopoModel):
    """Generated model for Marine."""

    bbox: Optional[Any] = Field(None)
    composition: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    name: Optional[Any] = Field(None)
    t50_fid: Optional[Any] = Field(None)
    type: Any = Field(...)
    updated_at: Any = Field(...)


class Nztopo50CartoText(BaseTopoModel):
    """Generated model for Nztopo50CartoText."""

    bbox: Optional[Any] = Field(None)
    chardistance: Optional[Any] = Field(None)
    charplace: Optional[Any] = Field(None)
    colour: Optional[Any] = Field(None)
    example_point_id: Optional[Any] = Field(None, description="id (UUID) of the example feature.")
    font: Optional[Any] = Field(None)
    full_text: Optional[Any] = Field(None)
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    labelanchor: Optional[Any] = Field(None)
    offset: Optional[Any] = Field(None)
    placement: Optional[Any] = Field(None)
    size: Optional[Any] = Field(None)
    style: Optional[Any] = Field(None)
    text_bend: Optional[Any] = Field(None)
    text_char_spacing_distance: Optional[Any] = Field(None)
    text_colour: Optional[Any] = Field(None)
    text_font: Optional[Any] = Field(None)
    text_height: Optional[Any] = Field(None)
    text_orientation: Optional[Any] = Field(None)
    text_placement: Optional[Any] = Field(None)
    text_size_type: Optional[Any] = Field(None)
    text_stretch_length: Optional[Any] = Field(None)
    text_string: Optional[Any] = Field(None)
    text_word_spacing_distance: Optional[Any] = Field(None)
    textanchor: Optional[Any] = Field(None)


class Nztopo50DmsGrid(BaseTopoModel):
    """Generated model for Nztopo50DmsGrid."""

    bbox: Optional[Any] = Field(None)
    direction: Optional[Any] = Field(None)
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    value: Optional[Any] = Field(None)


class Nztopo50Grid(BaseTopoModel):
    """Generated model for Nztopo50Grid."""

    bbox: Optional[Any] = Field(None)
    direction: Optional[Any] = Field(None)
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    value: Optional[Any] = Field(None)


class Nztopo50MapSheet(BaseTopoModel):
    """Generated model for Nztopo50MapSheet."""

    bbox: Optional[Any] = Field(None)
    example_point_id: str = Field(..., description="topo_id (UUID) of the example feature.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    published_at: Any = Field(...)
    published_version: str = Field(...)
    sheet_code: str = Field(...)
    sheet_name: str = Field(...)
    t50_fid: Optional[Any] = Field(None)
    type: str = Field(...)
    updated_at: Any = Field(...)
    x_origin: float = Field(...)
    y_origin: float = Field(...)


class PlacePoint(BaseTopoModel):
    """Generated model for PlacePoint."""

    bbox: Optional[Any] = Field(None)
    composition: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    description: Optional[Any] = Field(None)
    elevation: Optional[Any] = Field(None)
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    name: Optional[Any] = Field(None)
    orientation: Optional[Any] = Field(None)
    place_type: Optional[Any] = Field(None)
    status: Optional[Any] = Field(None)
    substance_extracted: Optional[Any] = Field(None)
    t50_fid: Optional[Any] = Field(None)
    type: Any = Field(...)
    updated_at: Any = Field(...)


class ProductIdentity(BaseTopoModel):
    """Generated model for ProductIdentity."""

    id: str = Field(..., description="UUID for the feature.")


class RailwayLine(BaseTopoModel):
    """All mainline railway lines are held in the Topo50 data and shown on the Topo50 printed maps. 
Where a railway line is located close to a road, the line held in the data and shown on the printed map 
may be offset from the road sufficient that the two symbols are recognisable at 1:50,000.

Multiple sidings may be held in the data and shown on the printed maps as a single feature"""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(..., description="ISO Datetime of when the feature was created")
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    name: Any = Field(..., description="The name of the railway line if known")
    railway_use: Any = Field(...)
    status: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    track_type: Any = Field(...)
    type: str = Field(...)
    updated_at: str = Field(..., description="ISO Datetime of when the feature was last updated")
    vehicle_type: Any = Field(...)


class RailwayPoint(BaseTopoModel):
    """Generated model for RailwayPoint."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(..., description="ISO Datetime of when the feature was created")
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    name: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    updated_at: str = Field(..., description="ISO Datetime of when the feature was last updated")


class RailwayStation(BaseTopoModel):
    """Generated model for RailwayStation."""

    bbox: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    name: Optional[Any] = Field(None)
    t50_fid: Optional[Any] = Field(None)
    type: str = Field(...)
    updated_at: Any = Field(...)


class Relief(BaseTopoModel):
    """Generated model for Relief."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(..., description="ISO Datetime of when the feature was created")
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    height: Any = Field(...)
    id: str = Field(..., description="UUIDv7 of the feature")
    name: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(..., description="ISO Datetime of when the feature was last updated")


class ReliefLine(BaseTopoModel):
    """Generated model for ReliefLine."""

    bbox: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    height: Optional[Any] = Field(None)
    id: str = Field(..., description="UUID for the feature.")
    name: Optional[Any] = Field(None)
    relief_use: Optional[Any] = Field(None)
    t50_fid: Optional[Any] = Field(None)
    type: Any = Field(...)
    updated_at: Any = Field(...)


class ReliefPoint(BaseTopoModel):
    """Generated model for ReliefPoint."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(..., description="ISO Datetime of when the feature was created")
    display: Any = Field(...)
    elevation: Any = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    height: Any = Field(...)
    id: str = Field(..., description="UUIDv7 of the feature")
    name: Any = Field(...)
    orientation: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(..., description="ISO Datetime of when the feature was last updated")


class ResidentialArea(BaseTopoModel):
    """Generated model for ResidentialArea."""

    bbox: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    name: Optional[Any] = Field(None)
    t50_fid: Optional[Any] = Field(None)
    type: str = Field(...)
    updated_at: Any = Field(...)


class RoadLine(BaseTopoModel):
    """Generated model for RoadLine."""

    bbox: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    hierarchy: Optional[Any] = Field(None)
    highway_number: Optional[Any] = Field(None)
    id: str = Field(..., description="UUID for the feature.")
    lane_count: Optional[Any] = Field(None)
    name: Optional[Any] = Field(None)
    name_id: Optional[Any] = Field(None)
    rna_sufi: Optional[Any] = Field(None)
    road_access: Optional[Any] = Field(None)
    status: Optional[Any] = Field(None)
    surface: Optional[Any] = Field(None)
    t50_fid: Optional[Any] = Field(None)
    type: str = Field(...)
    updated_at: Any = Field(...)
    way_count: Optional[Any] = Field(None)
    width_indicator: Optional[Any] = Field(None)


class Runway(BaseTopoModel):
    """Generated model for Runway."""

    bbox: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    runway_use: Optional[Any] = Field(None)
    status: Optional[Any] = Field(None)
    surface: Optional[Any] = Field(None)
    t50_fid: Optional[Any] = Field(None)
    type: str = Field(...)
    updated_at: Any = Field(...)


class Structure(BaseTopoModel):
    """Generated model for Structure."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(..., description="ISO Datetime of when the feature was created")
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    lid_type: Any = Field(...)
    name: Any = Field(...)
    species: Any = Field(...)
    status: Any = Field(...)
    stored_item: Any = Field(...)
    subtype: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(..., description="ISO Datetime of when the feature was last updated")


class StructureLine(BaseTopoModel):
    """Generated model for StructureLine."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(..., description="ISO Datetime of when the feature was created")
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    material: Any = Field(...)
    material_conveyed: Any = Field(...)
    name: Any = Field(...)
    restrictions: Any = Field(...)
    species: Any = Field(...)
    status: Any = Field(...)
    structure_use: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(..., description="ISO Datetime of when the feature was last updated")


class StructurePoint(BaseTopoModel):
    """Generated model for StructurePoint."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(..., description="ISO Datetime of when the feature was created")
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    height: Any = Field(...)
    id: str = Field(..., description="UUIDv7 of the feature")
    location: Any = Field(...)
    material: Any = Field(...)
    name: Any = Field(...)
    orientation: Any = Field(...)
    restrictions: Any = Field(...)
    status: Any = Field(...)
    stored_item: Any = Field(...)
    structure_use: Any = Field(...)
    subtype: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    tank_type: Any = Field(...)
    type: Any = Field(...)
    updated_at: str = Field(..., description="ISO Datetime of when the feature was last updated")
    wreck_of: Any = Field(...)


class TopoIdentity(BaseTopoModel):
    """Identity columns shared by every file in the dataset."""

    id: str = Field(..., description="UUID for the feature.")


class TrackLine(BaseTopoModel):
    """Generated model for TrackLine."""

    bbox: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    name: Optional[Any] = Field(None)
    status: Optional[Any] = Field(None)
    t50_fid: Optional[Any] = Field(None)
    track_type: Optional[Any] = Field(None)
    track_use: Optional[Any] = Field(None)
    type: str = Field(...)
    updated_at: Any = Field(...)


class TransportPoint(BaseTopoModel):
    """Generated model for TransportPoint."""

    bbox: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    name: Optional[Any] = Field(None)
    t50_fid: Optional[Any] = Field(None)
    type: Any = Field(...)
    updated_at: Any = Field(...)


class TrigPoint(BaseTopoModel):
    """Generated model for TrigPoint."""

    bbox: Optional[Any] = Field(None)
    code: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    elevation: Optional[Any] = Field(None)
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    name: Optional[Any] = Field(None)
    t50_fid: Optional[Any] = Field(None)
    trig_type: Optional[Any] = Field(None)
    type: str = Field(...)
    updated_at: Any = Field(...)


class TunnelLine(BaseTopoModel):
    """Generated model for TunnelLine."""

    bbox: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    name: Optional[Any] = Field(None)
    status: Optional[Any] = Field(None)
    subtype: Optional[Any] = Field(None)
    t50_fid: Optional[Any] = Field(None)
    tunnel_use: Optional[Any] = Field(None)
    tunnel_use2: Optional[Any] = Field(None)
    type: str = Field(...)
    updated_at: Any = Field(...)


class UtilityLine(BaseTopoModel):
    """Generated model for UtilityLine."""

    bbox: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    name: Optional[Any] = Field(None)
    status: Optional[Any] = Field(None)
    support_type: Optional[Any] = Field(None)
    t50_fid: Optional[Any] = Field(None)
    type: Any = Field(...)
    updated_at: Any = Field(...)
    utility_use: Optional[Any] = Field(None)
    visibility: Optional[Any] = Field(None)


class UtilityPoint(BaseTopoModel):
    """Generated model for UtilityPoint."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(..., description="ISO Datetime of when the feature was created")
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    name: Any = Field(...)
    orientation: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(..., description="ISO Datetime of when the feature was last updated")


class Vegetation(BaseTopoModel):
    """Generated model for Vegetation."""

    bbox: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    species: Optional[Any] = Field(None)
    subtype: Optional[Any] = Field(None)
    t50_fid: Optional[Any] = Field(None)
    type: Any = Field(...)
    updated_at: Any = Field(...)


class VegetationLine(BaseTopoModel):
    """Generated model for VegetationLine."""

    bbox: Optional[Any] = Field(None)
    created_at: Any = Field(..., description="Default at write time: today.")
    geometry: Any = Field(..., description="WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere.")
    id: str = Field(..., description="UUID for the feature.")
    t50_fid: Optional[Any] = Field(None)
    type: str = Field(...)
    updated_at: Any = Field(...)


class VegetationPoint(BaseTopoModel):
    """Generated model for VegetationPoint."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(..., description="ISO Datetime of when the feature was created")
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(..., description="ISO Datetime of when the feature was last updated")


class Water(BaseTopoModel):
    """Generated model for Water."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(..., description="ISO Datetime of when the feature was created")
    elevation: Any = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    group_name: Any = Field(...)
    height: Any = Field(...)
    hierarchy: Any = Field(...)
    id: str = Field(..., description="UUIDv7 of the feature")
    name: Any = Field(...)
    nzgb_feat_id: Any = Field(...)
    perennial: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    temperature_indicator: Any = Field(...)
    type: Any = Field(...)
    updated_at: str = Field(..., description="ISO Datetime of when the feature was last updated")
    water_use: Any = Field(...)


class WaterLine(BaseTopoModel):
    """Generated model for WaterLine."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(..., description="ISO Datetime of when the feature was created")
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    hierarchy: Any = Field(...)
    id: str = Field(..., description="UUIDv7 of the feature")
    name: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    type: Any = Field(...)
    updated_at: str = Field(..., description="ISO Datetime of when the feature was last updated")


class WaterPoint(BaseTopoModel):
    """Generated model for WaterPoint."""

    bbox: Optional[Any] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
    created_at: str = Field(..., description="ISO Datetime of when the feature was created")
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    id: str = Field(..., description="UUIDv7 of the feature")
    name: Any = Field(...)
    orientation: Any = Field(...)
    t50_fid: Any = Field(..., description="Reference topo50 feature ID.

Will be null if the feature is new and has not been published in a Topo50 edition.")
    temperature_indicator: Any = Field(...)
    type: Any = Field(...)
    updated_at: str = Field(..., description="ISO Datetime of when the feature was last updated")
