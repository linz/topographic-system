import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/tunnel_line")
@extension("additionalProperties", false)
model tunnel_line {
  ...TopographicFeature;
  type: tunnel_type;
  subtype: "livestock" | null;
  construction_type: tunnel_construction_type | null;
  status: tunnel_status | null;
  name: string | null;
  theme: Theme.Transport;
  ...TopographicMetadata;
  ...GeoParquet;
}

enum tunnel_construction_type {
  Natural: "natural",
  Manmade: "manmade",
}

enum tunnel_type {
  Cablecar: "cablecar",
  FootTraffic: "foot_traffic",
  Livestock: "livestock",
  River: "river",
  Train: "train",
  Tram: "tram",
  Vehicle: "vehicle",
}

enum tunnel_status {
  Closed: "closed",
  Historic: "historic",
  Disused: "disused",
  Derelict: "derelict",
  UnderConstruction: "under_construction",
}
