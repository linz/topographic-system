import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/structure_line")
@extension("additionalProperties", false)
model structure_line {
  ...TopographicFeature;

  type: structure_line_type;
  structure_use: structure_line_use | null;
  species: structure_line_species | null;
  status: structure_line_status | null;
  name: string | null;
  material: structure_line_material | null;
  material_conveyed: structure_line_material_conveyed | null;
  restrictions: structure_line_restrictions | null;

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

enum structure_line_use {
  Disused: "disused",
  Historic: "historic",
  Jetty: "jetty",
  PowerGeneration: "power generation",
  TunnelVentilation: "tunnel ventilation"
}

enum structure_line_species {
  Mussels: "mussels",
  Salmon: "salmon"
}

enum structure_line_status {
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
  UnderConstruction: "under construction"
}

enum structure_line_material {
  Concrete: "concrete",
  Rock: "rock"
}

enum structure_line_material_conveyed {
  Coal: "coal",
  Lime: "lime"
}

enum structure_line_restrictions {
  Locked: "locked"
}


