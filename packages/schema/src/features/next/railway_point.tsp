import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/railway_point")
@extension("additionalProperties", false)
model railway_point {
  ...TopographicFeature;

  type: "station";
  name: string | null;

  ...TopographicMetadata;
  ...GeoParquet
}

