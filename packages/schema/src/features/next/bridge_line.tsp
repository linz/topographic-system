import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/bridge_line")
@extension("additionalProperties", false)
model bridge_line {
  ...TopographicFeature;

  type: bridge_type | null;
  subtype: bridge_subtype | null;
  construction_type: construction_type | null;
  status: status | null;
  name: string | null;

  theme: "transport";
  ...TopographicMetadata;
  ...GeoParquet
}

enum bridge_type {
  Cablecar: "cablecar",
  Farm: "farm",
  FootTraffic: "foot_traffic",
  Train: "train",
  Vehicle: "vehicle",
}

enum bridge_subtype {
  FootTraffic: "foot_traffic",
  Train: "train",
}

enum construction_type {
  Suspension: "suspension",
  Swing: "swing",
  Trestle: "trestle",
}

enum status {
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
  UnderConstruction: "under_construction",
}

