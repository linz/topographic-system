import "@typespec/json-schema";
import "./common.tsp";
import "./structure.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/structure_line")
@extension("additionalProperties", false)
model structure_line {
  ...TopographicFeature;

  type: structure_line_type;
  subtype: structure_subtype | null;
  species: structure_species | null;
  status: structure_status | null;
  name: string | null;

  ...TopographicMetadata;
  ...GeoParquet
}

enum structure_line_type {
  Boatramp: "boatramp",
  Boom: "boom",
  Breakwater: "breakwater",
  CablewayIndustrial: "cableway_industrial",
  CablewayPeople: "cableway_people",
  Dam: "dam",
  Flume: "flume",
  Ladder: "ladder",
  MarineFarm: "marine_farm",
  SkiLift: "ski_lift",
  SkiTow: "ski_tow",
  Slipway: "slipway",
  SpillwayEdge: "spillway_edge",
  Walkwire: "walkwire",
  WaterRace: "water_race",
  Weir: "weir",
  Wharf: "wharf",
  WharfEdge: "wharf_edge"
}





