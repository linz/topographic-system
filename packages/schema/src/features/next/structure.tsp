import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/structure")
@extension("additionalProperties", false)
model structure {
  ...TopographicFeature;

  type: structure_type;
  lid_type: lid_type | null;
  tank_type: string | null;
  subtype: structure_subtype | null;
  species: structure_species | null;
  status: structure_status | null;
  name: string | null;
  stored_item: structure_stored_item | null;

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

enum lid_type {
  Covered: "covered"
}

enum structure_subtype {
  Lighthouse: "lighthouse",
  SurgeChamber: "surge chamber",
  Uncovered: "uncovered",
  Water: "water"
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

enum structure_stored_item {
  Fuel: "fuel",
  Water: "water"
}