import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/marine")
@extension("additionalProperties", false)
model marine {
  ...TopographicFeature;

  type: "mangrove" | "reef" | "rock" | "shoal";
  name: string | null;
  composition: string | null;
  
  theme: "landcover";
  ...TopographicMetadata;
  ...GeoParquet
}

