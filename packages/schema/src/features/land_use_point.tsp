import "@typespec/json-schema";
import "./common.tsp";
import "./land_use.tsp";

using TypeSpec.JsonSchema;

namespace Linz.Topography;

@jsonSchema("land_use_point")
@extension("additionalProperties", false)
model land_use_point {
  ...TopographicFeature;
  type: land_use_type;
  subtype: land_use_subtype | null;
  status: land_use_status | null;
  name: string | null;
  substance_extracted: substance_extracted | null;
  theme: Theme.Landuse;
  ...TopographicMetadata;
  ...GeoParquet;
}
