import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/nztopo50_grid")
@extension("additionalProperties", false)
model nztopo50_grid {
  ...ProductFeature;
  
  direction: grid_direction | null;
  value: float64 | null;

  ...GeoParquet
}

enum grid_direction {
  X: "x",
  Y: "y",
}