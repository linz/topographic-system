"""
Explicit Pydantic model class definitions for topographic features.

Generated from JSON schemas with proper Field constraints and type hints.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class BaseTopoModel(BaseModel):
    """Base class for all topographic feature models."""

    model_config = ConfigDict(extra="forbid")



class AirportBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class Airport(BaseTopoModel):
    __doc__ = "Generated model for Airport."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['airport'] = Field(...)
    name: Optional[str] = Field(...)
    theme: Literal['transport'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[AirportBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class BridgeLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class BridgeLine(BaseTopoModel):
    __doc__ = "Generated model for BridgeLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Optional[Literal['cablecar', 'farm', 'foot_traffic', 'train', 'vehicle']] = Field(...)
    subtype: Optional[Literal['foot_traffic', 'train']] = Field(...)
    construction_type: Optional[Literal['suspension', 'swing', 'trestle']] = Field(...)
    status: Optional[Literal['closed', 'dangerous', 'derelict', 'disused', 'historic', 'locked', 'old', 'private', 'remains', 'ruins', 'under_construction']] = Field(...)
    name: Optional[str] = Field(...)
    theme: Literal['transport'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[BridgeLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class BuildingBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class Building(BaseTopoModel):
    __doc__ = "Generated model for Building."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['building'] = Field(...)
    subtype: Optional[Literal['abattoir', 'camp', 'cement_works', 'church', 'energy_facility', 'factory', 'fertiliser_works', 'fire_lookout', 'forest_headquarters', 'gas_compound', 'greenhouse', 'gun_club', 'gun_emplacement', 'hall', 'homestead', 'hospital', 'hut', 'lodge', 'marae', 'mill', 'museum', 'observatory', 'polytechnic', 'power_generation', 'prison', 'private_hut', 'salt_works', 'school', 'shingle_works', 'shelter', 'silo', 'stamping_battery', 'substation', 'surf_club', 'university', 'visitor_centre', 'water_treatment_plant']] = Field(...)
    status: Optional[Literal['derelict', 'historic', 'private']] = Field(...)
    name: Optional[str] = Field(...)
    theme: Literal['buildings'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[BuildingBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class BuildingPointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class BuildingPoint(BaseTopoModel):
    __doc__ = "Generated model for BuildingPoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['building'] = Field(...)
    subtype: Optional[Literal['abattoir', 'camp', 'cement_works', 'church', 'energy_facility', 'factory', 'fertiliser_works', 'fire_lookout', 'forest_headquarters', 'gas_compound', 'greenhouse', 'gun_club', 'gun_emplacement', 'hall', 'homestead', 'hospital', 'hut', 'lodge', 'marae', 'mill', 'museum', 'observatory', 'polytechnic', 'power_generation', 'prison', 'private_hut', 'salt_works', 'school', 'shingle_works', 'shelter', 'silo', 'stamping_battery', 'substation', 'surf_club', 'university', 'visitor_centre', 'water_treatment_plant']] = Field(...)
    status: Optional[Literal['derelict', 'historic', 'private']] = Field(...)
    name: Optional[str] = Field(...)
    orientation: Optional[float] = Field(...)
    theme: Literal['buildings'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[BuildingPointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class CoastlineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class Coastline(BaseTopoModel):
    __doc__ = "Generated model for Coastline."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Union[Literal['coastline'], Literal['water_confluence'], Literal['wharf']] = Field(...)
    elevation: Optional[int] = Field(...)
    theme: Literal['landcover'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[CoastlineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class ContourBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class Contour(BaseTopoModel):
    __doc__ = "Generated model for Contour."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['contour'] = Field(...)
    elevation: Optional[int] = Field(...)
    definition: Optional[str] = Field(...)
    designation: Optional[Literal['supplementary']] = Field(...)
    formation: Optional[Literal['depression']] = Field(...)
    theme: Literal['relief'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[ContourBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class DescriptiveTextBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class DescriptiveText(BaseTopoModel):
    __doc__ = "Generated model for DescriptiveText."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['descriptive_text'] = Field(...)
    info_display: Optional[str] = Field(...)
    size: Optional[float] = Field(...)
    theme: Literal['annotation'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[DescriptiveTextBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class FenceLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class FenceLine(BaseTopoModel):
    __doc__ = "Generated model for FenceLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['fence'] = Field(...)
    theme: Literal['structures'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[FenceLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class FerryLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class FerryLine(BaseTopoModel):
    __doc__ = "Generated model for FerryLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['ferry_crossing'] = Field(...)
    subtype: Optional[Union[Literal['vehicle'], Literal['passenger'], Literal['freight']]] = Field(...)
    name: Optional[str] = Field(...)
    theme: Literal['transport'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[FerryLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class GeographicNameBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class GeographicName(BaseTopoModel):
    __doc__ = "Generated model for GeographicName."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['geographic_name'] = Field(...)
    name: Optional[str] = Field(...)
    desc_code: Optional[str] = Field(...)
    size: Optional[float] = Field(...)
    theme: Literal['annotation'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[GeographicNameBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class IslandBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class Island(BaseTopoModel):
    __doc__ = "Generated model for Island."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['island'] = Field(...)
    name: Optional[str] = Field(...)
    group_name: Optional[str] = Field(...)
    theme: Literal['landcover'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[IslandBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class LandcoverBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class Landcover(BaseTopoModel):
    __doc__ = "Generated model for Landcover."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['ice', 'moraine', 'moraine_wall', 'mud', 'sand', 'scree', 'shingle', 'swamp'] = Field(...)
    name: Optional[str] = Field(...)
    theme: Literal['landcover'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[LandcoverBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class LandcoverLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class LandcoverLine(BaseTopoModel):
    __doc__ = "Generated model for LandcoverLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['dredge_tailing'] = Field(...)
    theme: Literal['landcover'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[LandcoverLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class BBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class LandcoverPointCoreTypesBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class LandcoverPointCoreTypes(BaseTopoModel):
    __doc__ = "Generated model for LandcoverPointCoreTypes."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['cemetery', 'fumarole', 'swamp'] = Field(...)
    orientation: Any = Field(...)
    elevation: Any = Field(...)
    subtype: Any = Field(...)
    name: Optional[str] = Field(...)
    theme: Literal['landcover'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[LandcoverPointCoreTypesBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class RockOutcropBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class RockOutcrop(BaseTopoModel):
    __doc__ = "Generated model for RockOutcrop."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['rock_outcrop'] = Field(...)
    orientation: float = Field(...)
    elevation: int = Field(...)
    subtype: Literal['small_rock_outcrop', 'large_rock_outcrop', 'large_boulder'] = Field(...)
    name: Optional[str] = Field(...)
    theme: Literal['landcover'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[RockOutcropBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class LanduseBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class Landuse(BaseTopoModel):
    __doc__ = "Generated model for Landuse."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['cemetery', 'cycle_track', 'dog_track', 'golf_course', 'gravel_pit', 'horse_track', 'landfill', 'mine', 'orchard', 'pumice_pit', 'quarry', 'racetrack', 'rifle_range', 'showground', 'sportsfield', 'vineyard', 'vehicle_track'] = Field(...)
    subtype: Optional[Literal['training', 'opencast', 'underground']] = Field(...)
    status: Optional[Literal['dangerous', 'disused', 'old']] = Field(...)
    name: Optional[str] = Field(...)
    substance_extracted: Optional[Literal['bentonite', 'clay', 'coal', 'gold', 'gravel', 'ironsand', 'lime', 'limestone', 'metal', 'quartz', 'scheelite', 'shingle', 'silica_sand', 'silver', 'stone', 'zeolite']] = Field(...)
    theme: Literal['landuse'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[LanduseBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class LanduseLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class LanduseLine(BaseTopoModel):
    __doc__ = "Generated model for LanduseLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['cemetery', 'cycle_track', 'dog_track', 'golf_course', 'gravel_pit', 'horse_track', 'landfill', 'mine', 'orchard', 'pumice_pit', 'quarry', 'racetrack', 'rifle_range', 'showground', 'sportsfield', 'vineyard', 'vehicle_track'] = Field(...)
    subtype: Optional[Literal['training', 'opencast', 'underground']] = Field(...)
    name: Optional[str] = Field(...)
    theme: Literal['landuse'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[LanduseLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class LandusePointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class LandusePoint(BaseTopoModel):
    __doc__ = "Generated model for LandusePoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['cemetery', 'cycle_track', 'dog_track', 'golf_course', 'gravel_pit', 'horse_track', 'landfill', 'mine', 'orchard', 'pumice_pit', 'quarry', 'racetrack', 'rifle_range', 'showground', 'sportsfield', 'vineyard', 'vehicle_track'] = Field(...)
    subtype: Optional[Literal['training', 'opencast', 'underground']] = Field(...)
    status: Optional[Literal['dangerous', 'disused', 'old']] = Field(...)
    name: Optional[str] = Field(...)
    substance_extracted: Optional[Literal['bentonite', 'clay', 'coal', 'gold', 'gravel', 'ironsand', 'lime', 'limestone', 'metal', 'quartz', 'scheelite', 'shingle', 'silica_sand', 'silver', 'stone', 'zeolite']] = Field(...)
    theme: Literal['landuse'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[LandusePointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class MarineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class Marine(BaseTopoModel):
    __doc__ = "Generated model for Marine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Union[Literal['mangrove'], Literal['reef'], Literal['rock'], Literal['shoal']] = Field(...)
    subtype: Optional[Literal['coral', 'limestone', 'pumice', 'rock']] = Field(...)
    name: Optional[str] = Field(...)
    theme: Literal['landcover'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[MarineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class MarinePointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class MarinePoint(BaseTopoModel):
    __doc__ = "Generated model for MarinePoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['rock'] = Field(...)
    subtype: Optional[Literal['coral', 'limestone', 'pumice', 'rock']] = Field(...)
    name: Optional[str] = Field(...)
    theme: Literal['landcover'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[MarinePointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class Nztopo50CartoTextBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class Nztopo50CartoText(BaseTopoModel):
    __doc__ = "Generated model for Nztopo50CartoText."

    id: str = Field(..., description="UUIDv7 of the feature")
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    example_point_id: Optional[str] = Field(..., description="id (UUID) of the example feature.")
    full_text: Optional[str] = Field(...)
    text_bend: Optional[int] = Field(...)
    text_char_spacing_distance: Optional[int] = Field(...)
    text_colour: Optional[int] = Field(...)
    text_font: Optional[Literal['ATTrium-Italic', 'ATTriumMou-Cond', 'ATTriumMou-CondBold', 'ATTriumMou-CondItalic', 'ATTriumMou-Italic', 'ATTriumMou-Regular', 'Courier Bold Oblique']] = Field(...)
    text_height: Optional[float] = Field(...)
    text_orientation: Optional[float] = Field(...)
    text_placement: Optional[int] = Field(...)
    text_size_type: Optional[int] = Field(...)
    text_stretch_length: Optional[int] = Field(...)
    text_string: Optional[str] = Field(...)
    text_word_spacing_distance: Optional[int] = Field(...)
    font: Optional[Literal['', 'Nimbus Sans LINZ']] = Field(...)
    style: Optional[Literal['', 'Italic', 'Narrow', 'Narrow Bold', 'Narrow Italic', 'Regular']] = Field(...)
    colour: Optional[Literal['', 'black', 'process_blue', 'red']] = Field(...)
    size: Optional[float] = Field(...)
    placement: Optional[Literal['', 'AL', 'BL', 'OL']] = Field(...)
    offset: Optional[float] = Field(...)
    textanchor: Optional[Literal['', 'centre', 'left', 'left ', 'left 0.62', 'right']] = Field(...)
    labelanchor: Optional[float] = Field(...)
    charplace: Optional[Literal['', 'CharactersAtVertices', 'StretchCharacterSpacingToFit', 'StretchWordSpacingToFit']] = Field(...)
    chardistance: Optional[float] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[Nztopo50CartoTextBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class Nztopo50CoastlineIslandBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class Nztopo50CoastlineIsland(BaseTopoModel):
    __doc__ = "Derived polygon Union of coastline and island"

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Union[Literal['coastline'], Literal['island']] = Field(...)
    elevation: Optional[int] = Field(...)
    name: Optional[str] = Field(...)
    group_name: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[Nztopo50CoastlineIslandBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class Nztopo50DmsGridBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class Nztopo50DmsGrid(BaseTopoModel):
    __doc__ = "Generated model for Nztopo50DmsGrid."

    id: str = Field(..., description="UUIDv7 of the feature")
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    direction: Literal['x', 'y'] = Field(...)
    value: float = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[Nztopo50DmsGridBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class Nztopo50GridBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class Nztopo50Grid(BaseTopoModel):
    __doc__ = "Generated model for Nztopo50Grid."

    id: str = Field(..., description="UUIDv7 of the feature")
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    direction: Literal['x', 'y'] = Field(...)
    value: float = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[Nztopo50GridBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class Nztopo50IceContourBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class Nztopo50IceContour(BaseTopoModel):
    __doc__ = "Generated model for Nztopo50IceContour."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['contour_ice'] = Field(...)
    elevation: Optional[int] = Field(...)
    definition: Optional[str] = Field(...)
    designation: Optional[Literal['supplementary']] = Field(...)
    formation: Optional[Literal['depression']] = Field(...)
    theme: Literal['relief'] = Field(...)
    contour_id: str = Field(..., description="UUID for the intersecting contour feature.")
    landcover_id: str = Field(..., description="UUID for the intersecting landcover feature.")
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[Nztopo50IceContourBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class Nztopo50MapSheetBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class Nztopo50MapSheet(BaseTopoModel):
    __doc__ = "Generated model for Nztopo50MapSheet."

    id: str = Field(..., description="UUIDv7 of the feature")
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['nztopo50_map_sheet'] = Field(...)
    sheet_code: str = Field(...)
    sheet_name: str = Field(...)
    origin_x: float = Field(...)
    origin_y: float = Field(...)
    example_point_id: str = Field(..., description="id (UUID) of the example feature.")
    published_version: str = Field(...)
    published_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[Nztopo50MapSheetBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class Nztopo50RockLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class Nztopo50RockLine(BaseTopoModel):
    __doc__ = "Generated model for Nztopo50RockLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    marine_id: str = Field(..., description="UUID of the source `marine` rock polygon this boundary line was derived from.")
    type: Literal['rock'] = Field(...)
    name: Optional[str] = Field(...)
    sub_type: Optional[Literal['coral', 'limestone', 'pumice', 'rock']] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[Nztopo50RockLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class Nztopo50SeaPolygonBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class Nztopo50SeaPolygon(BaseTopoModel):
    __doc__ = "Derived sea (moana) polygons for the water layer.\n\nThe land polygons (coastline and island) are inverted and sliced by Web\nMercator quadkey tiles so no single large polygon exists."

    id: str = Field(...)
    type: Literal['moana'] = Field(...)
    quadkey: str = Field(..., description="Web Mercator quadkey of the tile this sea polygon was sliced to.")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[Nztopo50SeaPolygonBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class PlacePointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class PlacePoint(BaseTopoModel):
    __doc__ = "Generated model for PlacePoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['cave', 'grave', 'historic_site', 'monument', 'pa'] = Field(...)
    subtype: Optional[Literal['coral', 'limestone', 'pumice', 'rock']] = Field(...)
    name: Optional[str] = Field(...)
    theme: Literal['landuse'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[PlacePointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class RailwayLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class RailwayLine(BaseTopoModel):
    __doc__ = "All mainline railway lines are held in the Topo50 data and shown on the Topo50 printed maps.\nWhere a railway line is located close to a road, the line held in the data and shown on the printed map\nmay be offset from the road sufficient that the two symbols are recognisable at 1:50,000.\n\nMultiple sidings may be held in the data and shown on the printed maps as a single feature"

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['railway'] = Field(...)
    subtype: Optional[Literal['siding']] = Field(...)
    track_type: Optional[Literal['single', 'multiple']] = Field(...)
    vehicle_type: Optional[Literal['train', 'tram', 'rail_cart', 'cablecar']] = Field(...)
    status: Optional[Literal['disused']] = Field(...)
    name: Optional[str] = Field(...)
    theme: Literal['transport'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[RailwayLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class RailwayPointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class RailwayPoint(BaseTopoModel):
    __doc__ = "Generated model for RailwayPoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['station'] = Field(...)
    name: Optional[str] = Field(...)
    theme: Literal['transport'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[RailwayPointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class ReliefBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class Relief(BaseTopoModel):
    __doc__ = "Generated model for Relief."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Union[Literal['waterfall'], Literal['rapid']] = Field(...)
    name: Optional[str] = Field(...)
    height: Optional[float] = Field(...)
    theme: Literal['relief'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[ReliefBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class ReliefLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class ReliefLine(BaseTopoModel):
    __doc__ = "Generated model for ReliefLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['cliff_edge', 'cutting_edge', 'embankment', 'rapid', 'slip_edge', 'waterfall', 'waterfall_edge', 'seawall'] = Field(...)
    subtype: Optional[Literal['causeway', 'stopbank']] = Field(...)
    name: Optional[str] = Field(...)
    height: Optional[float] = Field(...)
    theme: Literal['relief'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[ReliefLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class ReliefPointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class ReliefPoint(BaseTopoModel):
    __doc__ = "Generated model for ReliefPoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['height', 'saddle', 'sinkhole', 'waterfall'] = Field(...)
    name: Optional[str] = Field(...)
    elevation: Optional[int] = Field(...)
    height: Optional[float] = Field(...)
    orientation: Optional[float] = Field(...)
    theme: Literal['relief'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[ReliefPointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class ResidentialAreaBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class ResidentialArea(BaseTopoModel):
    __doc__ = "Generated model for ResidentialArea."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['residential_area'] = Field(...)
    name: Optional[str] = Field(...)
    theme: Literal['landuse'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[ResidentialAreaBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class RoadLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class RoadLine(BaseTopoModel):
    __doc__ = "Generated model for RoadLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['road'] = Field(...)
    hierarchy: Optional[str] = Field(...)
    status: Optional[Literal['under_construction', 'closed']] = Field(...)
    name: Optional[str] = Field(...)
    highway_number: Optional[str] = Field(...)
    lane_count: Optional[int] = Field(...)
    surface: Optional[Literal['metalled', 'unmetalled', 'sealed']] = Field(...)
    way_count: Optional[Literal['one_way']] = Field(...)
    width_indicator: Optional[Literal['w']] = Field(...)
    road_access: Optional[Literal['mp']] = Field(...)
    theme: Literal['transport'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[RoadLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class RunwayBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class Runway(BaseTopoModel):
    __doc__ = "Generated model for Runway."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['runway'] = Field(...)
    subtype: Optional[Literal['aerodrome', 'airport', 'airstrip']] = Field(...)
    status: Optional[Literal['disused']] = Field(...)
    surface: Optional[Literal['grass', 'sealed']] = Field(...)
    theme: Literal['transport'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[RunwayBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class StructureBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class Structure(BaseTopoModel):
    __doc__ = "Generated model for Structure."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['dry_dock', 'fish_farm', 'marine_farm', 'reservoir', 'siphon', 'tank'] = Field(...)
    subtype: Optional[Literal['building', 'coal', 'concrete', 'disused', 'fuel', 'historic', 'jetty', 'lake', 'land', 'lighthouse', 'lime', 'locked', 'plane', 'power_generation', 'rock', 'sea', 'ship', 'surge_chamber', 'tunnel_ventilation', 'uncovered', 'water']] = Field(...)
    lid_type: Optional[Literal['covered']] = Field(...)
    tank_type: Optional[Literal['uncovered', 'water', 'surge_chamber']] = Field(...)
    species: Optional[Literal['mussels', 'salmon']] = Field(...)
    status: Optional[Literal['closed', 'dangerous', 'derelict', 'disused', 'historic', 'locked', 'old', 'private', 'remains', 'ruins', 'under_construction']] = Field(...)
    name: Optional[str] = Field(...)
    theme: Literal['structures'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[StructureBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class StructureLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class StructureLine(BaseTopoModel):
    __doc__ = "Generated model for StructureLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['boatramp', 'boom', 'breakwater', 'cableway_industrial', 'cableway_people', 'dam', 'flume', 'ladder', 'marine_farm', 'ski_lift', 'ski_tow', 'slipway', 'spillway_edge', 'walkwire', 'water_race', 'weir', 'wharf', 'wharf_edge'] = Field(...)
    subtype: Optional[Literal['building', 'coal', 'concrete', 'disused', 'fuel', 'historic', 'jetty', 'lake', 'land', 'lighthouse', 'lime', 'locked', 'plane', 'power_generation', 'rock', 'sea', 'ship', 'surge_chamber', 'tunnel_ventilation', 'uncovered', 'water']] = Field(...)
    species: Optional[Literal['mussels', 'salmon']] = Field(...)
    status: Optional[Literal['closed', 'dangerous', 'derelict', 'disused', 'historic', 'locked', 'old', 'private', 'remains', 'ruins', 'under_construction']] = Field(...)
    name: Optional[str] = Field(...)
    theme: Literal['structures'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[StructureLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class StructurePointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class StructurePoint(BaseTopoModel):
    __doc__ = "Generated model for StructurePoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['beacon', 'bivouac', 'buoy', 'chimney', 'dredge', 'flare', 'floodgate', 'gate', 'ladder', 'lighthouse', 'mast', 'radar_dome', 'redoubt', 'satellite_station', 'shaft', 'siphon', 'stockyard', 'tank', 'tower', 'well', 'windmill', 'wreck'] = Field(...)
    subtype: Optional[Literal['building', 'coal', 'concrete', 'disused', 'fuel', 'historic', 'jetty', 'lake', 'land', 'lighthouse', 'lime', 'locked', 'plane', 'power_generation', 'rock', 'sea', 'ship', 'surge_chamber', 'tunnel_ventilation', 'uncovered', 'water']] = Field(...)
    tank_type: Optional[Literal['uncovered', 'water', 'surge_chamber']] = Field(...)
    status: Optional[Literal['closed', 'dangerous', 'derelict', 'disused', 'historic', 'locked', 'old', 'private', 'remains', 'ruins', 'under_construction']] = Field(...)
    name: Optional[str] = Field(...)
    height: Optional[float] = Field(...)
    orientation: Optional[float] = Field(...)
    theme: Literal['structures'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[StructurePointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class TrackLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class TrackLine(BaseTopoModel):
    __doc__ = "Generated model for TrackLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['track'] = Field(...)
    subtype: Optional[Literal['cycle_only', 'foot', 'vehicle']] = Field(...)
    track_type: Optional[Literal['connector', 'multiple', 'route', 'single']] = Field(...)
    status: Optional[Literal['closed']] = Field(...)
    name: Optional[str] = Field(...)
    theme: Literal['transport'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[TrackLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class TransportPointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class TransportPoint(BaseTopoModel):
    __doc__ = "Generated model for TransportPoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Union[Literal['ford'], Literal['helipad']] = Field(...)
    name: Optional[str] = Field(...)
    theme: Literal['transport'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[TransportPointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class TrigPointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class TrigPoint(BaseTopoModel):
    __doc__ = "Generated model for TrigPoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['trig'] = Field(...)
    trig_type: Optional[Literal['beaconed']] = Field(...)
    name: Optional[str] = Field(...)
    code: Optional[str] = Field(...)
    elevation: Optional[int] = Field(...)
    theme: Literal['relief'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[TrigPointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class TunnelLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class TunnelLine(BaseTopoModel):
    __doc__ = "Generated model for TunnelLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['cablecar', 'foot_traffic', 'livestock', 'river', 'train', 'tram', 'vehicle'] = Field(...)
    subtype: Optional[Literal['livestock']] = Field(...)
    construction_type: Optional[Literal['natural', 'manmade']] = Field(...)
    status: Optional[Literal['closed', 'historic', 'disused', 'derelict', 'under_construction']] = Field(...)
    name: Optional[str] = Field(...)
    theme: Literal['transport'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[TunnelLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class UtilityLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class UtilityLine(BaseTopoModel):
    __doc__ = "Generated model for UtilityLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Union[Literal['pipeline'], Literal['powerline'], Literal['telephone']] = Field(...)
    subtype: Optional[Literal['ironsand', 'sewage', 'steam', 'water']] = Field(...)
    support_type: Optional[Literal['pole', 'pylon']] = Field(...)
    status: Optional[str] = Field(...)
    visibility: Optional[Literal['underground']] = Field(...)
    theme: Literal['utility'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[UtilityLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class UtilityPointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class UtilityPoint(BaseTopoModel):
    __doc__ = "Generated model for UtilityPoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['gas_valve', 'geo_bore', 'pylon'] = Field(...)
    orientation: Optional[float] = Field(...)
    theme: Literal['utility'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[UtilityPointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class VegetationBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class Vegetation(BaseTopoModel):
    __doc__ = "Generated model for Vegetation."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Union[Literal['exotic'], Literal['native'], Literal['scattered_scrub'], Literal['scrub']] = Field(...)
    subtype: Optional[Literal['coniferous', 'non-coniferous']] = Field(...)
    theme: Literal['landcover'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[VegetationBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class VegetationLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class VegetationLine(BaseTopoModel):
    __doc__ = "Generated model for VegetationLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['shelter_belt'] = Field(...)
    theme: Literal['landcover'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[VegetationLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class VegetationPointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class VegetationPoint(BaseTopoModel):
    __doc__ = "Generated model for VegetationPoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Union[Literal['tree'], Literal['scattered_scrub']] = Field(...)
    theme: Literal['landcover'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[VegetationPointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class WaterBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class Water(BaseTopoModel):
    __doc__ = "Generated model for Water."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['canal', 'drain', 'lagoon', 'lake', 'pond', 'river', 'soakhole', 'spring', 'waterfall'] = Field(...)
    subtype: Optional[Literal['evaporation', 'hydro_electric', 'ice_skating', 'oil', 'oxidation', 'reservoir', 'settling', 'sewage', 'sewage_treatment', 'sludge']] = Field(...)
    name: Optional[str] = Field(...)
    group_name: Optional[str] = Field(...)
    height: Optional[float] = Field(...)
    elevation: Optional[int] = Field(...)
    perennial: Optional[Literal['dry', 'seasonal']] = Field(...)
    temperature_indicator: Optional[Literal['cold', 'hot']] = Field(...)
    theme: Literal['landcover'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[WaterBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class WaterLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class WaterLine(BaseTopoModel):
    __doc__ = "Generated model for WaterLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['canal', 'drain', 'lagoon', 'lake', 'pond', 'river', 'soakhole', 'spring', 'waterfall'] = Field(...)
    name: Optional[str] = Field(...)
    theme: Literal['landcover'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[WaterLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class WaterPointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class WaterPoint(BaseTopoModel):
    __doc__ = "Generated model for WaterPoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: datetime = Field(...)
    updated_at: datetime = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Literal['canal', 'drain', 'lagoon', 'lake', 'pond', 'river', 'soakhole', 'spring', 'waterfall'] = Field(...)
    name: Optional[str] = Field(...)
    height: Optional[float] = Field(...)
    orientation: Optional[float] = Field(...)
    temperature_indicator: Optional[Literal['cold', 'hot']] = Field(...)
    theme: Literal['landcover'] = Field(...)
    metadata: Optional[str] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[WaterPointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
