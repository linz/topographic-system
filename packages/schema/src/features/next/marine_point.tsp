import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/marine_point")
@extension("additionalProperties", false)
model marine_point {
  ...TopographicFeature;
  type: "rock";
  subtype: composition_subtype | null;
  name: string | null;
  theme: Theme.Landcover;
  ...TopographicMetadata;
  ...GeoParquet;
}
