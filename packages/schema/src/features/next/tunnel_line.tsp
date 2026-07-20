import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/tunnel_line")
@extension("additionalProperties", false)
model tunnel_line {
  ...TopographicFeature;

  type: "tunnel";
  tunnel_use: tunnel_use | null;
  tunnel_use2: tunnel_use | null;
  subtype: tunnel_subtype | null;
  status: tunnel_status | null;
  name: string | null;
  
  ...TopographicMetadata;
  ...GeoParquet
}

enum tunnel_subtype {
  Natural: "natural",
}

enum tunnel_use {
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
  UnderConstruction: "under_construction",
}