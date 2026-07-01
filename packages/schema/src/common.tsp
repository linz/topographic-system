import "@typespec/json-schema";

using TypeSpec.JsonSchema;

/**
 * Shared building blocks. Each per-feature model spreads the mixins
 * it needs, so the emitted JSON Schemas remain flat objects with
 * additionalProperties: false.
 */
namespace Topo;

// ---- Auto-detected enums (column-level, < 20 distinct values across dataset) ----

/** Auto-derived from observed values of `use1`. */
enum BridgeUse1Enum {
  Cablecar: "cablecar",
  Farm: "farm",
  FootTraffic: "foot traffic",
  Train: "train",
  Vehicle: "vehicle",
}

/** Auto-derived from observed values of `use2`. */
enum BridgeUse2Enum {
  Train: "train",
  FootTraffic: "foot traffic",
}

/** Auto-derived from observed values of `charplace`. */
enum CharplaceEnum {
  _: "",
  CharactersAtVertices: "CharactersAtVertices",
  StretchCharacterSpacingToFit: "StretchCharacterSpacingToFit",
  StretchWordSpacingToFit: "StretchWordSpacingToFit",
}

/** Auto-derived from observed values of `colour`. */
enum ColourEnum {
  _: "",
  Black: "black",
  ProcessBlue: "process_blue",
  Red: "red",
}

/** Auto-derived from observed values of `composition`. */
enum CompositionEnum {
  Limestone: "limestone",
  Pumice: "pumice",
}

/** Auto-derived from observed values of `construction_type`. */
enum ConstructionTypeEnum {
  Suspension: "suspension",
  Swing: "swing",
  Trestle: "trestle",
}

/** Auto-derived from observed values of `designation`. */
enum DesignationEnum {
  Supplementary: "supplementary",
}

/** Auto-derived from observed values of `direction`. */
enum DirectionEnum {
  X: "x",
  Y: "y",
}

/** Auto-derived from observed values of `display`. */
enum DisplayEnum {
  _0: "0",
  _1: "1",
  _2: "2",
  _3: "3",
}

/** Auto-derived from observed values of `example_class`. */
enum ExampleClassEnum {
  GeographicName: "geographic_name",
  TrigPnt: "trig_pnt",
}

/** Auto-derived from observed values of `font`. */
enum FontEnum {
  _: "",
  NimbusSansLinz: "Nimbus Sans LINZ",
}

/** Auto-derived from observed values of `formation`. */
enum FormationEnum {
  Depression: "depression",
}

/** Auto-derived from observed values of `infrastructure_use`. */
enum UtilityUseEnum {
  Ironsand: "ironsand",
  Sewage: "sewage",
  Steam: "steam",
  Water: "water",
}

/** Auto-derived from observed values of `subtype`. */
enum LanduseSubTypeEnum {
  Opencast: "opencast",
  Training: "training",
}

/** Auto-derived from observed values of `landuse_use`. */
enum LanduseUseEnum {
  Cycle: "cycle",
  Dog: "dog",
  Horse: "horse",
  Vehicle: "vehicle",
}

/** Auto-derived from observed values of `lid_type`. */
enum LidTypeEnum {
  Covered: "covered",
}

/** Auto-derived from observed values of `location`. */
enum LocationEnum {
  Lake: "lake",
  Land: "land",
  Sea: "sea",
}

/** Auto-derived from observed values of `material`. */
enum MaterialEnum {
  Concrete: "concrete",
  Rock: "rock",
}

/** Auto-derived from observed values of `material_conveyed`. */
enum MaterialConveyedEnum {
  Coal: "coal",
  Lime: "lime",
}

/** Auto-derived from observed values of `perennial`. */
enum PerennialEnum {
  Dry: "dry",
  Seasonal: "seasonal",
}

/** Auto-derived from observed values of `place_type`. */
enum PlaceTypeEnum {
  Opencast: "opencast",
  Underground: "underground",
}

/** Auto-derived from observed values of `placement`. */
enum PlacementEnum {
  _: "",
  AL: "AL",
  BL: "BL",
  OL: "OL",
}

/** Auto-derived from observed values of `railway_use`. */
enum RailwayUseEnum {
  Siding: "siding",
}

/** Auto-derived from observed values of `relief_use`. */
enum ReliefUseEnum {
  Causeway: "causeway",
  Stopbank: "stopbank",
}

/** Auto-derived from observed values of `restrictions`. */
enum RestrictionsEnum {
  Locked: "locked",
}

/** Auto-derived from observed values of `road_access`. */
enum RoadAccessEnum {
  Mp: "mp",
}

/** Auto-derived from observed values of `runway_use`. */
enum RunwayUseEnum {
  Aerodrome: "aerodrome",
  Airport: "airport",
  Airstrip: "airstrip",
}

/** Auto-derived from observed values of `species`. */
enum SpeciesEnum {
  Mussels: "mussels",
  Salmon: "salmon",
}

/** Auto-derived from observed values of `status`. */
enum StatusEnum {
  Closed: "closed",
  Dangerous: "dangerous",
  Derelict: "derelict",
  Disused: "disused",
  Historic: "historic",
  Locked: "locked",
  Old: "old",
  Private: "private",
  Remains: "remains",
  Ruins: "ruins",
  UnderConstruction: "under construction",
}

/** Auto-derived from observed values of `stored_item`. */
enum StoredItemEnum {
  Fuel: "fuel",
  Water: "water",
}

/** Auto-derived from observed values of `subtype`. */
enum StructureSubTypeEnum {
  Lighthouse: "lighthouse",
  SurgeChamber: "surge chamber",
  Uncovered: "uncovered",
  Water: "water",
}

/** Auto-derived from observed values of `structure_use`. */
enum StructureUseEnum {
  Disused: "disused",
  Historic: "historic",
  Jetty: "jetty",
  PowerGeneration: "power generation",
  TunnelVentilation: "tunnel ventilation",
}

/** Auto-derived from observed values of `style`. */
enum StyleEnum {
  _: "",
  Italic: "Italic",
  Narrow: "Narrow",
  NarrowBold: "Narrow Bold",
  NarrowItalic: "Narrow Italic",
  Regular: "Regular",
}

/** Auto-derived from observed values of `subtype`. */
enum SubTypeEnum {
  Coniferous: "coniferous",
  NonConiferous: "non-coniferous",
}

/** Auto-derived from observed values of `substance`. */
enum SubstanceEnum {
  Bentonite: "bentonite",
  Clay: "clay",
  Coal: "coal",
  Gold: "gold",
  Gravel: "gravel",
  Ironsand: "ironsand",
  Lime: "lime",
  Limestone: "limestone",
  Metal: "metal",
  Quartz: "quartz",
  Scheelite: "scheelite",
  Shingle: "shingle",
  SilicaSand: "silica sand",
  Silver: "silver",
  Stone: "stone",
  Zeolite: "zeolite",
}

/** Auto-derived from observed values of `support_type`. */
enum SupportTypeEnum {
  Pole: "pole",
  Pylon: "pylon",
}

/** Auto-derived from observed values of `surface`. */
enum SurfaceEnum {
  Grass: "grass",
  Metalled: "metalled",
  Sealed: "sealed",
  Unmetalled: "unmetalled",
}

/** Auto-derived from observed values of `temperature`. */
enum TemperatureEnum {
  Hot: "hot",
}

/** Auto-derived from observed values of `temperature_indicator`. */
enum TemperatureIndicatorEnum {
  Cold: "cold",
  Hot: "hot",
}

/** Auto-derived from observed values of `text_font`. */
enum TextFontEnum {
  ATTriumItalic: "ATTrium-Italic",
  ATTriumMouCond: "ATTriumMou-Cond",
  ATTriumMouCondBold: "ATTriumMou-CondBold",
  ATTriumMouCondItalic: "ATTriumMou-CondItalic",
  ATTriumMouItalic: "ATTriumMou-Italic",
  ATTriumMouRegular: "ATTriumMou-Regular",
  CourierBoldOblique: "Courier Bold Oblique",
}

/** Auto-derived from observed values of `textanchor`. */
enum TextanchorEnum {
  _: "",
  Centre: "centre",
  Left: "left",
  LeftWithSpace: "left ",  // Note trailing space; not a typo. TODO: Need to fix source data to remove this.
  Left062: "left 0.62",
  Right: "right",
}

/** Auto-derived from observed values of `track_type`. */
enum TrackTypeEnum {
  Connector: "connector",
  Multiple: "multiple",
  Route: "route",
  Single: "single",
}

/** Auto-derived from observed values of `track_use`. */
enum TrackUseEnum {
  CycleOnly: "cycle only",
  Foot: "foot",
  Vehicle: "vehicle",
}

/** Auto-derived from observed values of `trig_type`. */
enum TrigTypeEnum {
  Beacon: "beaconed",
}

/** Auto-derived from observed values of `subtype`. */
enum TunnelSubTypeEnum {
  Natural: "natural",
}

/** Auto-derived from observed values of `tunnel_use`. */
enum TunnelUseEnum {
  Cablecar: "cablecar",
  FootTraffic: "foot traffic",
  Livestock: "livestock",
  River: "river",
  Train: "train",
  Tram: "tram",
  Vehicle: "vehicle",
}

/** Auto-derived from observed values of `tunnel_use2`. */
enum TunnelUse2Enum {
  Livestock: "livestock",
}

/** Auto-derived from observed values of `vehicle_type`. */
enum VehicleTypeEnum {
  Cablecar: "cablecar",
  RailCart: "rail cart",
  Train: "train",
  Tram: "tram",
}

/** Auto-derived from observed values of `visibility`. */
enum VisibilityEnum {
  Underground: "underground",
}

/** Auto-derived from observed values of `water_use`. */
enum WaterUseEnum {
  Evaporation: "evaporation",
  HydroElectric: "hydro-electric",
  IceSkating: "ice skating",
  Oil: "oil",
  Oxidation: "oxidation",
  Reservoir: "reservoir",
  Settling: "settling",
  Sewage: "sewage",
  SewageTreatment: "sewage treatment",
  Sludge: "sludge",
}

/** Auto-derived from observed values of `way_count`. */
enum WayCountEnum {
  OneWay: "one way",
}

/** Auto-derived from observed values of `wreck_of`. */
enum WreckOfEnum {
  Plane: "plane",
  Ship: "ship",
}

/** GeoParquet 1.1 covering bbox struct. */
model BBox {
  xmin: float32;
  ymin: float32;
  xmax: float32;
  ymax: float32;
}

/** Identity columns shared by every file in the dataset. */
@jsonSchema
model TopoIdentity {
  /** UUID for the feature. */
  id: string;
}

@jsonSchema
model ProductIdentity {
  /** UUID for the feature. */
  id: string;
}

/** Capture / change-tracking columns shared by most features. */
@jsonSchema
model AuditFields {
  updated_at: {};
  /** Default at write time: today. */
  created_at: {};

}
