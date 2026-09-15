import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

namespace Topography;

@jsonSchema("contour")
@extension("additionalProperties", false)
model contour {
  ...TopographicFeature;
  type: "contour";
  elevation: int32 | null;
  definition: string | null;
  designation: "supplementary" | null;
  formation: "depression" | null;
  theme: Theme.Relief;
  ...TopographicMetadata;
  ...GeoParquet;
}
