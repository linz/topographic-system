import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/fence_line")
@extension("additionalProperties", false)
model fence_line {
  ...TopographicFeature;
  
  type: "fence";

  ...TopographicMetadata;
  ...GeoParquet
}

