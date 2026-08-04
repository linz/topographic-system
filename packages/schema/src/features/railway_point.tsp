import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("railway_point")
@extension("additionalProperties", false)
model railway_point {
  ...TopographicFeature;
  type: "station";
  name: string | null;
  theme: Theme.Transport;
  ...TopographicMetadata;
  ...GeoParquet;
}
