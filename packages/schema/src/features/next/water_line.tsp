import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/water_line")
@extension("additionalProperties", false)
model water_line {
  ...TopographicFeature;
  
  type: "canal" | "drain" | "river";
  hierarchy: string | null;
  name: string | null;

  ...GeoParquet
}

