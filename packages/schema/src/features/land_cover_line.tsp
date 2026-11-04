import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

namespace Linz.Topography;

@jsonSchema("land_cover_line")
@extension("additionalProperties", false)
model land_cover_line {
  ...TopographicFeature;
  type: "dredge_tailing";
  theme: Theme.Landcover;
  ...TopographicMetadata;
  ...GeoParquet;
}
