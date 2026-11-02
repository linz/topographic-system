import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("track_line")
@extension("additionalProperties", false)
model track_line {
  ...TopographicFeature;
  type: track_type | null;
  subtype: track_subtype | null;
  status: track_status | null;
  name: string | null;
  theme: Theme.Transport;
  ...TopographicMetadata;
  ...GeoParquet;
}

enum track_type {
  CycleOnly: "cycle_only",
  Foot: "foot",
  Vehicle: "vehicle",
}

enum track_subtype {
  Connector: "connector",
  Multiple: "multiple",
  Route: "route",
  Single: "single",
}

enum track_status {
  Closed: "closed",
}
