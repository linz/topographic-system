import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/ferry_line")
@extension("additionalProperties", false)
model ferry_line {
  ...TopographicFeature;

  type: "ferry_crossing";

  ...GeoParquet
}

