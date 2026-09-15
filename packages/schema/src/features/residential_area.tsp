import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

namespace Topography;

@jsonSchema("residential_area")
@extension("additionalProperties", false)
model residential_area {
  ...TopographicFeature;
  type: "residential_area";
  name: string | null;
  theme: Theme.Landuse;
  ...TopographicMetadata;
  ...GeoParquet;
}
