import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

namespace Linz.Topography;

@jsonSchema("fence_line")
@extension("additionalProperties", false)
model fence_line {
  ...TopographicFeature;
  type: "fence";
  theme: Theme.Structures;
  ...TopographicMetadata;
  ...GeoParquet;
}
