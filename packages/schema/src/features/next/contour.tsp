import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/contour")
@extension("additionalProperties", false)
model contour {
  ...TopographicFeature;

  type: "contour";
  elevation: int32 | null;
  definition: string | null;
  designation: "supplementary" | null;
  formation: "depression" | null;

  theme: "relief";
  ...TopographicMetadata;
  ...GeoParquet
}


