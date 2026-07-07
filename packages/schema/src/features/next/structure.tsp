import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/structure")
@extension("additionalProperties", false)
model structure {
  ...TopographicFeature;

  type: structure_type;
  subtype: structure_subtype | null;
  lid_type: "covered" | null;
  tank_type: structure_tank_type | null;
  species: structure_species | null;
  status: structure_status | null;
  name: string | null;

  ...GeoParquet
}

enum structure_type {
  DryDock: "dry_dock",
  FishFarm: "fish_farm",
  MarineFarm: "marine_farm",
  Reservoir: "reservoir",
  Siphon: "siphon",
  Tank: "tank"
}

enum structure_subtype {
  Building: "building",
  Coal: "coal",
  Concrete: "concrete",
  Disused: "disused",
  Fuel: "fuel",
  Historic: "historic",
  Jetty: "jetty",
  Lighthouse: "lighthouse",
  Lime: "lime",
  Locked: "locked",
  Plane: "plane",
  PowerGeneration: "power_generation",
  Rock: "rock",
  Ship: "ship",
  SurgeChamber: "surge chamber",
  TunnelVentilation: "tunnel_ventilation",
  Uncovered: "uncovered",
  Water: "water"
}

enum structure_tank_type {
  Uncovered: "uncovered",
  Water: "water",
  SurgeChamber: "surge chamber"
}

enum structure_species {
  Mussels: "mussels",
  Salmon: "salmon"
}

enum structure_status {
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



