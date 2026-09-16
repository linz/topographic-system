import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

namespace Topography;

@jsonSchema("coastline")
@extension("additionalProperties", false)
model coastline {
  ...TopographicFeature;
  type: "coastline" | "water_confluence" | "wharf";
  elevation: int32 | null;
  theme: Theme.Landcover;
  ...TopographicMetadata;
  ...GeoParquet;
}
