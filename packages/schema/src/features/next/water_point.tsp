import "@typespec/json-schema";
import "./common.tsp";
import "./water.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/water_point")
@extension("additionalProperties", false)
model water_point {
  ...TopographicFeature;

  type: water_type;
  name: string | null;
  orientation: float64 | null;
  temperature_indicator: water_temperature_indicator | null;

  ...TopographicMetadata;
  ...GeoParquet
}

