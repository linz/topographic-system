import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/ferry_crossing")
@extension("additionalProperties", false)
model ferry_crossing {
  ...TopographicFeature;

  type: "ferry_crossing";
  
  ...TopographicMetadata;
  ...GeoParquet
}

