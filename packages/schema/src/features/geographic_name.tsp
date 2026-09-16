import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

namespace Topography;

@jsonSchema("geographic_name")
@extension("additionalProperties", false)
model geographic_name {
  ...TopographicFeature;
  type: "geographic_name";
  name: string | null;
  desc_code: string | null;
  size: float64 | null;
  theme: Theme.Annotation;
  ...TopographicMetadata;
  ...GeoParquet;
}
