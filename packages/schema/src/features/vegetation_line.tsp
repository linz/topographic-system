import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

namespace Linz.Topography;

@jsonSchema("vegetation_line")
@extension("additionalProperties", false)
model vegetation_line {
  ...TopographicFeature;
  type: "shelter_belt";
  theme: Theme.Landcover;
  ...TopographicMetadata;
  ...GeoParquet;
}
