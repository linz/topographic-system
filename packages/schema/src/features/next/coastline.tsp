import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/coastline")
@extension("additionalProperties", false)
model coastline {
  ...TopographicFeature;
  
  type: "coastline" | "water_confluence" | "wharf";
  elevation: int32 | null;

  theme: "landcover";
  ...TopographicMetadata;
  ...GeoParquet
}


