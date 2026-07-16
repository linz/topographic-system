import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/airport")
@extension("additionalProperties", false)
model airport {
  ...TopographicFeature;

  type: "airport";
  name: string | null;

  ...TopographicMetadata;
  ...GeoParquet
}
