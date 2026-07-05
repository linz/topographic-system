import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/relief_point")
@extension("additionalProperties", false)
model relief_point {
  ...TopographicFeature;

  type: "height" | "sinkhole" | "waterfall";
  name: string | null;
  display: display_enum | null;
  elevation: int32 | null;
  height: float64 | null;
  orientation: float64 | null;

  ...GeoParquet
}

/** This enum will be redone in another work item - keeping as is for now */
enum display_enum {
  _0: "0",
  _1: "1",
  _2: "2",
  _3: "3",
}