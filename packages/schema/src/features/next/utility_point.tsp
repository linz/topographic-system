import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/utility_point")
@extension("additionalProperties", false)
model utility_point {
  ...TopographicFeature;

  type: "gas_valve" | "geo_bore" | "pylon" | "substation";
  name: string | null;
  orientation: float64 | null;

  ...GeoParquet
}

