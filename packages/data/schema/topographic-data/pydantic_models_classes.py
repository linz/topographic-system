"""
Explicit Pydantic model class definitions for topographic features.

Generated from JSON schemas with proper Field constraints and type hints.
"""

from __future__ import annotations

from typing import Any, Optional, Union

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


class AirportDataSource(BaseTopoModel):
    __doc__ = "Generated model for AirportDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class Airport(BaseTopoModel):
    __doc__ = "Generated model for Airport."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    name: Optional[str] = Field(...)
    metadata: Optional[list[AirportDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[AirportBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class BridgeLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class BridgeLineDataSource(BaseTopoModel):
    __doc__ = "Generated model for BridgeLineDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class BridgeLine(BaseTopoModel):
    __doc__ = "Generated model for BridgeLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: Optional[str] = Field(...)
    subtype: Optional[str] = Field(...)
    construction_type: Optional[str] = Field(...)
    status: Optional[str] = Field(...)
    name: Optional[str] = Field(...)
    metadata: Optional[list[BridgeLineDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[BridgeLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class BuildingBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class BuildingDataSource(BaseTopoModel):
    __doc__ = "Generated model for BuildingDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class Building(BaseTopoModel):
    __doc__ = "Generated model for Building."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    subtype: Optional[str] = Field(...)
    status: Optional[str] = Field(...)
    name: Optional[str] = Field(...)
    metadata: Optional[list[BuildingDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[BuildingBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class BuildingPointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class BuildingPointDataSource(BaseTopoModel):
    __doc__ = "Generated model for BuildingPointDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class BuildingPoint(BaseTopoModel):
    __doc__ = "Generated model for BuildingPoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    subtype: Optional[str] = Field(...)
    status: Optional[str] = Field(...)
    name: Optional[str] = Field(...)
    orientation: Optional[float] = Field(...)
    metadata: Optional[list[BuildingPointDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[BuildingPointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class CoastlineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class CoastlineDataSource(BaseTopoModel):
    __doc__ = "Generated model for CoastlineDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class Coastline(BaseTopoModel):
    __doc__ = "Generated model for Coastline."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    elevation: Optional[int] = Field(...)
    metadata: Optional[list[CoastlineDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[CoastlineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class ContourBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class ContourDataSource(BaseTopoModel):
    __doc__ = "Generated model for ContourDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class Contour(BaseTopoModel):
    __doc__ = "Generated model for Contour."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    elevation: Optional[int] = Field(...)
    definition: Optional[str] = Field(...)
    designation: Optional[str] = Field(...)
    formation: Optional[str] = Field(...)
    metadata: Optional[list[ContourDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[ContourBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class DescriptiveTextBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class DescriptiveTextDataSource(BaseTopoModel):
    __doc__ = "Generated model for DescriptiveTextDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class DescriptiveText(BaseTopoModel):
    __doc__ = "Generated model for DescriptiveText."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    info_display: Optional[str] = Field(...)
    size: Optional[float] = Field(...)
    metadata: Optional[list[DescriptiveTextDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[DescriptiveTextBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class FenceLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class FenceLineDataSource(BaseTopoModel):
    __doc__ = "Generated model for FenceLineDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class FenceLine(BaseTopoModel):
    __doc__ = "Generated model for FenceLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    metadata: Optional[list[FenceLineDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[FenceLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class FerryLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class FerryLineDataSource(BaseTopoModel):
    __doc__ = "Generated model for FerryLineDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class FerryLine(BaseTopoModel):
    __doc__ = "Generated model for FerryLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    subtype: Optional[str] = Field(...)
    name: Optional[str] = Field(...)
    metadata: Optional[list[FerryLineDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[FerryLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class GeographicNameBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class GeographicNameDataSource(BaseTopoModel):
    __doc__ = "Generated model for GeographicNameDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class GeographicName(BaseTopoModel):
    __doc__ = "Generated model for GeographicName."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    name: Optional[str] = Field(...)
    desc_code: Optional[str] = Field(...)
    size: Optional[float] = Field(...)
    metadata: Optional[list[GeographicNameDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[GeographicNameBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class IslandBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class IslandDataSource(BaseTopoModel):
    __doc__ = "Generated model for IslandDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class Island(BaseTopoModel):
    __doc__ = "Generated model for Island."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    name: Optional[str] = Field(...)
    group_name: Optional[str] = Field(...)
    metadata: Optional[list[IslandDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[IslandBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class LandcoverBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class LandcoverDataSource(BaseTopoModel):
    __doc__ = "Generated model for LandcoverDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class Landcover(BaseTopoModel):
    __doc__ = "Generated model for Landcover."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    name: Optional[str] = Field(...)
    metadata: Optional[list[LandcoverDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[LandcoverBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class LandcoverLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class LandcoverLineDataSource(BaseTopoModel):
    __doc__ = "Generated model for LandcoverLineDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class LandcoverLine(BaseTopoModel):
    __doc__ = "Generated model for LandcoverLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    metadata: Optional[list[LandcoverLineDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[LandcoverLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class BBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class DataSource(BaseTopoModel):
    __doc__ = "Generated model for DataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class LandcoverPointCoreTypesBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class LandcoverPointCoreTypesDataSource(BaseTopoModel):
    __doc__ = "Generated model for LandcoverPointCoreTypesDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class LandcoverPointCoreTypes(BaseTopoModel):
    __doc__ = "Generated model for LandcoverPointCoreTypes."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    subtype: Any = Field(...)
    metadata: Optional[list[LandcoverPointCoreTypesDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[LandcoverPointCoreTypesBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class RockOutcropBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class RockOutcropDataSource(BaseTopoModel):
    __doc__ = "Generated model for RockOutcropDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class RockOutcrop(BaseTopoModel):
    __doc__ = "Generated model for RockOutcrop."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    subtype: str = Field(...)
    metadata: Optional[list[RockOutcropDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[RockOutcropBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class LanduseBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class LanduseDataSource(BaseTopoModel):
    __doc__ = "Generated model for LanduseDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class Landuse(BaseTopoModel):
    __doc__ = "Generated model for Landuse."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    subtype: Optional[str] = Field(...)
    status: Optional[str] = Field(...)
    name: Optional[str] = Field(...)
    substance_extracted: Optional[str] = Field(...)
    metadata: Optional[list[LanduseDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[LanduseBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class LanduseLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class LanduseLineDataSource(BaseTopoModel):
    __doc__ = "Generated model for LanduseLineDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class LanduseLine(BaseTopoModel):
    __doc__ = "Generated model for LanduseLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    subtype: Optional[str] = Field(...)
    name: Optional[str] = Field(...)
    metadata: Optional[list[LanduseLineDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[LanduseLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class LandusePointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class LandusePointDataSource(BaseTopoModel):
    __doc__ = "Generated model for LandusePointDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class LandusePoint(BaseTopoModel):
    __doc__ = "Generated model for LandusePoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    subtype: Optional[str] = Field(...)
    status: Optional[str] = Field(...)
    name: Optional[str] = Field(...)
    substance_extracted: Optional[str] = Field(...)
    metadata: Optional[list[LandusePointDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[LandusePointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class MarineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class MarineDataSource(BaseTopoModel):
    __doc__ = "Generated model for MarineDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class Marine(BaseTopoModel):
    __doc__ = "Generated model for Marine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    name: Optional[str] = Field(...)
    composition: Optional[str] = Field(...)
    metadata: Optional[list[MarineDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[MarineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class MarinePointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class MarinePointDataSource(BaseTopoModel):
    __doc__ = "Generated model for MarinePointDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class MarinePoint(BaseTopoModel):
    __doc__ = "Generated model for MarinePoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    name: Optional[str] = Field(...)
    metadata: Optional[list[MarinePointDataSource]] = Field(...)
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
    text_font: Optional[str] = Field(...)
    text_height: Optional[float] = Field(...)
    text_orientation: Optional[float] = Field(...)
    text_placement: Optional[int] = Field(...)
    text_size_type: Optional[int] = Field(...)
    text_stretch_length: Optional[int] = Field(...)
    text_string: Optional[str] = Field(...)
    text_word_spacing_distance: Optional[int] = Field(...)
    font: Optional[str] = Field(...)
    style: Optional[str] = Field(...)
    colour: Optional[str] = Field(...)
    size: Optional[float] = Field(...)
    placement: Optional[str] = Field(...)
    offset: Optional[float] = Field(...)
    textanchor: Optional[str] = Field(...)
    labelanchor: Optional[float] = Field(...)
    charplace: Optional[str] = Field(...)
    chardistance: Optional[float] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[Nztopo50CartoTextBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


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
    direction: str = Field(...)
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
    direction: str = Field(...)
    value: float = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[Nztopo50GridBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


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
    type: str = Field(...)
    sheet_code: str = Field(...)
    sheet_name: str = Field(...)
    origin_x: float = Field(...)
    origin_y: float = Field(...)
    example_point_id: str = Field(..., description="id (UUID) of the example feature.")
    published_version: str = Field(...)
    published_at: str = Field(...)
    updated_at: str = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[Nztopo50MapSheetBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class PlacePointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class PlacePointDataSource(BaseTopoModel):
    __doc__ = "Generated model for PlacePointDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class PlacePoint(BaseTopoModel):
    __doc__ = "Generated model for PlacePoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    subtype: Optional[str] = Field(...)
    name: Optional[str] = Field(...)
    elevation: Optional[int] = Field(...)
    composition: Optional[str] = Field(...)
    description: Optional[str] = Field(...)
    metadata: Optional[list[PlacePointDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[PlacePointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class RailwayLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class RailwayLineDataSource(BaseTopoModel):
    __doc__ = "Generated model for RailwayLineDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class RailwayLine(BaseTopoModel):
    __doc__ = "All mainline railway lines are held in the Topo50 data and shown on the Topo50 printed maps. \nWhere a railway line is located close to a road, the line held in the data and shown on the printed map \nmay be offset from the road sufficient that the two symbols are recognisable at 1:50,000.\n\nMultiple sidings may be held in the data and shown on the printed maps as a single feature"

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    subtype: Optional[str] = Field(...)
    track_type: Optional[str] = Field(...)
    vehicle_type: Optional[str] = Field(...)
    status: Optional[str] = Field(...)
    name: Optional[str] = Field(...)
    metadata: Optional[list[RailwayLineDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[RailwayLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class RailwayPointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class RailwayPointDataSource(BaseTopoModel):
    __doc__ = "Generated model for RailwayPointDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class RailwayPoint(BaseTopoModel):
    __doc__ = "Generated model for RailwayPoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    name: Optional[str] = Field(...)
    metadata: Optional[list[RailwayPointDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[RailwayPointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class ReliefBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class ReliefDataSource(BaseTopoModel):
    __doc__ = "Generated model for ReliefDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class Relief(BaseTopoModel):
    __doc__ = "Generated model for Relief."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    name: Optional[str] = Field(...)
    height: Optional[float] = Field(...)
    metadata: Optional[list[ReliefDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[ReliefBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class ReliefLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class ReliefLineDataSource(BaseTopoModel):
    __doc__ = "Generated model for ReliefLineDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class ReliefLine(BaseTopoModel):
    __doc__ = "Generated model for ReliefLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    subtype: Optional[str] = Field(...)
    name: Optional[str] = Field(...)
    height: Optional[float] = Field(...)
    metadata: Optional[list[ReliefLineDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[ReliefLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class ReliefPointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class ReliefPointDataSource(BaseTopoModel):
    __doc__ = "Generated model for ReliefPointDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class ReliefPoint(BaseTopoModel):
    __doc__ = "Generated model for ReliefPoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    name: Optional[str] = Field(...)
    display: Optional[str] = Field(...)
    elevation: Optional[int] = Field(...)
    height: Optional[float] = Field(...)
    orientation: Optional[float] = Field(...)
    metadata: Optional[list[ReliefPointDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[ReliefPointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class ResidentialAreaBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class ResidentialAreaDataSource(BaseTopoModel):
    __doc__ = "Generated model for ResidentialAreaDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class ResidentialArea(BaseTopoModel):
    __doc__ = "Generated model for ResidentialArea."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    name: Optional[str] = Field(...)
    metadata: Optional[list[ResidentialAreaDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[ResidentialAreaBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class RoadLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class RoadLineDataSource(BaseTopoModel):
    __doc__ = "Generated model for RoadLineDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class RoadLine(BaseTopoModel):
    __doc__ = "Generated model for RoadLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    hierarchy: Optional[str] = Field(...)
    status: Optional[str] = Field(...)
    name: Optional[str] = Field(...)
    highway_number: Optional[str] = Field(...)
    lane_count: Optional[int] = Field(...)
    surface: Optional[str] = Field(...)
    way_count: Optional[str] = Field(...)
    width_indicator: Optional[str] = Field(...)
    road_access: Optional[str] = Field(...)
    metadata: Optional[list[RoadLineDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[RoadLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class RunwayBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class RunwayDataSource(BaseTopoModel):
    __doc__ = "Generated model for RunwayDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class Runway(BaseTopoModel):
    __doc__ = "Generated model for Runway."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    subtype: Optional[str] = Field(...)
    status: Optional[str] = Field(...)
    surface: Optional[str] = Field(...)
    metadata: Optional[list[RunwayDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[RunwayBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class StructureBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class StructureDataSource(BaseTopoModel):
    __doc__ = "Generated model for StructureDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class Structure(BaseTopoModel):
    __doc__ = "Generated model for Structure."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    subtype: Optional[str] = Field(...)
    lid_type: Optional[str] = Field(...)
    tank_type: Optional[str] = Field(...)
    species: Optional[str] = Field(...)
    status: Optional[str] = Field(...)
    name: Optional[str] = Field(...)
    metadata: Optional[list[StructureDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[StructureBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class StructureLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class StructureLineDataSource(BaseTopoModel):
    __doc__ = "Generated model for StructureLineDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class StructureLine(BaseTopoModel):
    __doc__ = "Generated model for StructureLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    subtype: Optional[str] = Field(...)
    species: Optional[str] = Field(...)
    status: Optional[str] = Field(...)
    name: Optional[str] = Field(...)
    metadata: Optional[list[StructureLineDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[StructureLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class StructurePointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class StructurePointDataSource(BaseTopoModel):
    __doc__ = "Generated model for StructurePointDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class StructurePoint(BaseTopoModel):
    __doc__ = "Generated model for StructurePoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    subtype: Optional[str] = Field(...)
    tank_type: Optional[str] = Field(...)
    status: Optional[str] = Field(...)
    name: Optional[str] = Field(...)
    height: Optional[float] = Field(...)
    orientation: Optional[float] = Field(...)
    metadata: Optional[list[StructurePointDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[StructurePointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class TrackLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class TrackLineDataSource(BaseTopoModel):
    __doc__ = "Generated model for TrackLineDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class TrackLine(BaseTopoModel):
    __doc__ = "Generated model for TrackLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    subtype: Optional[str] = Field(...)
    track_type: Optional[str] = Field(...)
    status: Optional[str] = Field(...)
    name: Optional[str] = Field(...)
    metadata: Optional[list[TrackLineDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[TrackLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class TransportPointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class TransportPointDataSource(BaseTopoModel):
    __doc__ = "Generated model for TransportPointDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class TransportPoint(BaseTopoModel):
    __doc__ = "Generated model for TransportPoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    name: Optional[str] = Field(...)
    metadata: Optional[list[TransportPointDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[TransportPointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class TrigPointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class TrigPointDataSource(BaseTopoModel):
    __doc__ = "Generated model for TrigPointDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class TrigPoint(BaseTopoModel):
    __doc__ = "Generated model for TrigPoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    trig_type: Optional[str] = Field(...)
    name: Optional[str] = Field(...)
    code: Optional[str] = Field(...)
    elevation: Optional[int] = Field(...)
    metadata: Optional[list[TrigPointDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[TrigPointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class TunnelLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class TunnelLineDataSource(BaseTopoModel):
    __doc__ = "Generated model for TunnelLineDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class TunnelLine(BaseTopoModel):
    __doc__ = "Generated model for TunnelLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    tunnel_use: Optional[str] = Field(...)
    tunnel_use2: Optional[str] = Field(...)
    subtype: Optional[str] = Field(...)
    status: Optional[str] = Field(...)
    name: Optional[str] = Field(...)
    metadata: Optional[list[TunnelLineDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[TunnelLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class UtilityLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class UtilityLineDataSource(BaseTopoModel):
    __doc__ = "Generated model for UtilityLineDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class UtilityLine(BaseTopoModel):
    __doc__ = "Generated model for UtilityLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    subtype: Optional[str] = Field(...)
    support_type: Optional[str] = Field(...)
    status: Optional[str] = Field(...)
    name: Optional[str] = Field(...)
    visibility: Optional[str] = Field(...)
    metadata: Optional[list[UtilityLineDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[UtilityLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class UtilityPointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class UtilityPointDataSource(BaseTopoModel):
    __doc__ = "Generated model for UtilityPointDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class UtilityPoint(BaseTopoModel):
    __doc__ = "Generated model for UtilityPoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    name: Optional[str] = Field(...)
    orientation: Optional[float] = Field(...)
    metadata: Optional[list[UtilityPointDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[UtilityPointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class VegetationBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class VegetationDataSource(BaseTopoModel):
    __doc__ = "Generated model for VegetationDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class Vegetation(BaseTopoModel):
    __doc__ = "Generated model for Vegetation."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    subtype: Optional[str] = Field(...)
    species: Optional[str] = Field(...)
    metadata: Optional[list[VegetationDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[VegetationBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class VegetationLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class VegetationLineDataSource(BaseTopoModel):
    __doc__ = "Generated model for VegetationLineDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class VegetationLine(BaseTopoModel):
    __doc__ = "Generated model for VegetationLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    metadata: Optional[list[VegetationLineDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[VegetationLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class VegetationPointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class VegetationPointDataSource(BaseTopoModel):
    __doc__ = "Generated model for VegetationPointDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class VegetationPoint(BaseTopoModel):
    __doc__ = "Generated model for VegetationPoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    metadata: Optional[list[VegetationPointDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[VegetationPointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class WaterBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class WaterDataSource(BaseTopoModel):
    __doc__ = "Generated model for WaterDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class Water(BaseTopoModel):
    __doc__ = "Generated model for Water."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    subtype: Optional[str] = Field(...)
    hierarchy: Optional[str] = Field(...)
    name: Optional[str] = Field(...)
    group_name: Optional[str] = Field(...)
    height: Optional[float] = Field(...)
    elevation: Optional[int] = Field(...)
    perennial: Optional[str] = Field(...)
    temperature_indicator: Optional[str] = Field(...)
    metadata: Optional[list[WaterDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[WaterBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class WaterLineBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class WaterLineDataSource(BaseTopoModel):
    __doc__ = "Generated model for WaterLineDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class WaterLine(BaseTopoModel):
    __doc__ = "Generated model for WaterLine."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    hierarchy: Optional[str] = Field(...)
    name: Optional[str] = Field(...)
    metadata: Optional[list[WaterLineDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[WaterLineBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")


class WaterPointBBox(BaseTopoModel):
    __doc__ = "GeoParquet 1.1 covering bbox struct."

    xmin: float = Field(...)
    ymin: float = Field(...)
    xmax: float = Field(...)
    ymax: float = Field(...)


class WaterPointDataSource(BaseTopoModel):
    __doc__ = "Generated model for WaterPointDataSource."

    table_column: str = Field(..., description="Name of the column in this table where the linked data gets copied to")
    source: str = Field(..., description="Registered source for linked data")
    source_key_name: str = Field(..., description="Name of the key column in source to use for linking")
    source_key_value: Union[int, str] = Field(..., description="Value of the key column in source to use for linking.")
    source_table: str = Field(..., description="Name of the table in source that contains the linked data")
    source_column: str = Field(..., description="Name of the column in the source where the linked data gets copied from")
    source_updated_at: str = Field(..., description="Timestamp when the source was last updated")
    imported_at: str = Field(..., description="Timestamp when this linked data was last imported")


class WaterPoint(BaseTopoModel):
    __doc__ = "Generated model for WaterPoint."

    id: str = Field(..., description="UUIDv7 of the feature")
    created_at: str = Field(...)
    updated_at: str = Field(...)
    t50_fid: Optional[int] = Field(..., description="Reference topo50 feature ID.\n\nWill be null if the feature is new and has not been published in a Topo50 edition.")
    type: str = Field(...)
    name: Optional[str] = Field(...)
    orientation: Optional[float] = Field(...)
    temperature_indicator: Optional[str] = Field(...)
    metadata: Optional[list[WaterPointDataSource]] = Field(...)
    geometry: Any = Field(..., description="GeoParquet 1.1 covering geometry struct.")
    bbox: Optional[WaterPointBBox] = Field(None, description="GeoParquet 1.1 covering bbox struct.")
