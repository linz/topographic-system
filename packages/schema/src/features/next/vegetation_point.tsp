import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/vegetation_point")
@extension("additionalProperties", false)
model vegetation_point {
  ...TopographicFeature;

  type: "tree" | "scattered_scrub";

  ...GeoParquet
}

