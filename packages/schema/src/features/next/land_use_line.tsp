import "@typespec/json-schema";
import "./common.tsp";
import "./land_use.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/land_use_line")
@extension("additionalProperties", false)
model land_use_line {
  ...TopographicFeature;
  type: land_use_type;
  subtype: land_use_subtype | null;
  name: string | null;
  theme: Theme.Landuse;
  ...TopographicMetadata;
  ...GeoParquet;
}
