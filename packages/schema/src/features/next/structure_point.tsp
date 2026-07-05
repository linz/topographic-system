import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/structure_point")
@extension("additionalProperties", false)
model structure_point {
  ...TopographicFeature;

  type: structure_point_type;
  structure_use: structure_point_use | null;
  tank_type: "uncovered" | "water" | "surge chamber" | null;
  subtype: structure_point_subtype | null;
  status: structure_point_status | null;
  name: string | null;
  location: structure_point_location | null;
  height: float64 | null;
  orientation: float64 | null;
  material: structure_point_material | null;
  restrictions: structure_point_restrictions | null;
  stored_item: structure_point_stored_item | null;
  wreck_of: wreck_of | null;

  ...GeoParquet
}

enum structure_point_type {
  Beacon: "beacon",
  Bivouac: "bivouac",
  Buoy: "buoy",
  Chimney: "chimney",
  Dredge: "dredge",
  Flare: "flare",
  Floodgate: "floodgate",
  Gate: "gate",
  Ladder: "ladder",
  Mast: "mast",
  Redoubt: "redoubt",
  Shaft: "shaft",
  Siphon: "siphon",
  Stockyard: "stockyard",
  Tank: "tank",
  Tower: "tower",
  Well: "well",
  Windmill: "windmill",
  Wreck: "wreck"
}

enum structure_point_use {
  Disused: "disused",
  Historic: "historic",
  Jetty: "jetty",
  PowerGeneration: "power generation",
  TunnelVentilation: "tunnel ventilation"
}

enum subtype {
  Lighthouse: "lighthouse",
  SurgeChamber: "surge chamber",
  Uncovered: "uncovered",
  Water: "water"
}

enum structure_point_subtype {
  Lighthouse: "lighthouse",
  SurgeChamber: "surge chamber",
  Uncovered: "uncovered",
  Water: "water"
}

enum structure_point_species {
  Mussels: "mussels",
  Salmon: "salmon"
}

enum structure_point_status {
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

enum structure_point_location {
  Lake: "lake",
  Land: "land",
  Sea: "sea"
}

enum structure_point_material {
  Concrete: "concrete",
  Rock: "rock"
}

enum structure_point_restrictions {
  Locked: "locked"
}

enum structure_point_stored_item {
  Fuel: "fuel",
  Water: "water"
}

enum wreck_of {
  Plane: "plane",
  Ship: "ship"
}
