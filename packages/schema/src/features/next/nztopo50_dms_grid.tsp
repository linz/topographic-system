import "@typespec/json-schema";
import "./common.tsp";
import "./nztopo50_grid.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/nztopo50_dms_grid")
@extension("additionalProperties", false)
model nztopo50_dms_grid {
  ...ProductFeature;

  direction: grid_direction | null;
  value: float64 | null;

  ...GeoParquet
}
