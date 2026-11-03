import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/geographic_name")
@extension("additionalProperties", false)
model geographic_name {
  ...TopographicFeature;
  type: geographic_name_type;
  name: string | null;
  size: float64;
  theme: Theme.Annotation;
  ...TopographicMetadata;
  ...GeoParquet;
}

enum geographic_name_type {
  AIR: "airport",
  BAY: "bay_inlet",
  BCH: "beach",
  BDGE: "bridge",
  BLDG: "building",
  BNK: "bank",
  BSN: "mountain_basin",
  CAVE: "cave",
  CEM: "cemetery",
  CGND: "camping_ground",
  CLIF: "cliff",
  CRTR: "crater",
  DAM: "dam",
  ESTY: "estuary",
  FALL: "waterfall",
  FARM: "farm",
  FLAT: "flat",
  FORD: "ford",
  FRST: "forest",
  GEO: "geothermal_area",
  GLCR: "glacier",
  GLKS: "golf_links",
  GORG: "gorge_valley",
  HILL: "hill",
  HIST: "historic",
  HOSP: "hospital",
  HSTD: "homestead",
  HUT: "hut",
  ISLD: "island",
  ISTH: "isthmus",
  LAKE: "lake",
  LOC: "locality",
  LTH: "lighthouse",
  MCUL: "miscellaneous_cultural_feature",
  METR: "metropolis",
  MINE: "mine",
  MNAT: "miscellaneous_natural_feature",
  MPLA: "miscellaneous_place",
  MRFM: "marine_rock_formation",
  NAVB: "navigational_beacon",
  NLR: "native_land_reserve",
  PA: "pa",
  PARK: "park",
  PASS: "pass",
  PEN: "peninsula",
  PNT: "point",
  POPL: "populated_place",
  PPLN: "pipeline",
  RC: "racecourse",
  RDGE: "ridge",
  RDRF: "road_related_feature",
  RLYF: "railway_related_feature",
  RNGE: "range",
  ROCK: "rock",
  RSTN: "railway_station",
  SBRB: "suburb",
  SCH: "school",
  SEA: "sea",
  SKI: "ski",
  SLIP: "slip",
  SPR: "spring",
  STRA: "strait",
  STRM: "stream",
  SWMP: "swamp",
  TOWN: "town",
  TRCK: "track",
  TREE: "tree",
  TRIG: "trig",
  TUNL: "tunnel",
  TWAY: "tramway",
  UNIV: "university",
  USAT: "urban_satellite",
  WELL: "well",
  WHF: "wharf",
  WILD: "wilderness_area",
  WRCK: "wreck",
  WWAY: "water_way",
}
