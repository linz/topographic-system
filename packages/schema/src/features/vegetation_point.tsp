import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("vegetation_point")
@extension("additionalProperties", false)
model vegetation_point {
  ...TopographicFeature;
  type: "tree" | "scattered_scrub";
  theme: Theme.Landcover;
  ...TopographicMetadata;
  ...GeoParquet;
}
