import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("transport_point")
@extension("additionalProperties", false)
model transport_point {
  ...TopographicFeature;
  type: "ford" | "helipad";
  name: string | null;
  theme: Theme.Transport;
  ...TopographicMetadata;
  ...GeoParquet;
}
