import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/relief_line")
@extension("additionalProperties", false)
model relief_line {
  ...TopographicFeature;
  type: relief_line_type;
  subtype: relief_line_subtype | null;
  name?: string | null;
  height?: float64 | null;

  ...GeoParquet
}

enum relief_line_type {
  CliffEdge: "cliff_edge";
  CuttingEdge: "cutting_edge";
  Embankment: "embankment";
  Rapid: "rapid";
  SlipEdge: "slip_edge";
  Waterfall: "waterfall";
  WaterfallEdge: "waterfall_edge";
}

enum relief_line_subtype {
  Causeway: "causeway",
  Stopbank: "stopbank",
}