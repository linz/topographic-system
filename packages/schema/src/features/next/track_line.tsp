import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/track_line")
@extension("additionalProperties", false)
model track_line {
  ...TopographicFeature;

  type: "track";
  subtype: track_subtype | null;
  track_type: track_type | null;
  status: track_status | null;
  name: string | null;
  
  theme: "transport";
  ...TopographicMetadata;
  ...GeoParquet
}

enum track_subtype {
  CycleOnly: "cycle_only",
  Foot: "foot",
  Vehicle: "vehicle",
}

enum track_type {
  Connector: "connector",
  Multiple: "multiple",
  Route: "route",
  Single: "single",
}

enum track_status {
  Closed: "closed",
}

