import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("airport")
@extension("additionalProperties", false)
model airport {
  ...TopographicFeature;
  type: "airport";
  name: string | null;
  theme: Theme.Transport;
  ...TopographicMetadata;
  ...GeoParquet;
}
