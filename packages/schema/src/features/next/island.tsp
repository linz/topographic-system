import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/island")
@extension("additionalProperties", false)
model island {
  ...TopographicFeature;

  type: "island";
  name: string | null;
  group_name: string | null;
  
  ...TopographicMetadata;
  ...GeoParquet
}

