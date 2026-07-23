import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/descriptive_text")
@extension("additionalProperties", false)
model descriptive_text {
  ...TopographicFeature;

  type: "descriptive_text";
  info_display: string | null;
  size: float64 | null;
  
  theme: "annotation";
  ...TopographicMetadata;
  ...GeoParquet
}


