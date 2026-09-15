import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

namespace Topography;

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
