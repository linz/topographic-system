import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/transport_point")
@extension("additionalProperties", false)
model transport_point {
  ...TopographicFeature;

  type: "ford" | "helipad";
  name: string | null;
  
  theme: "transport";
  ...TopographicMetadata;
  ...GeoParquet
}


