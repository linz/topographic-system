import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/vegetation_line")
@extension("additionalProperties", false)
model vegetation_line {
  ...TopographicFeature;

  type: "shelter_belt";
  
  ...TopographicMetadata;
  ...GeoParquet
}

