import "@typespec/json-schema";
import "./common.tsp";
import "./water.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/water_line")
@extension("additionalProperties", false)
model water_line {
  ...TopographicFeature;
  
  type: water_type;
  hierarchy: string | null;
  name: string | null;

  ...TopographicMetadata;
  ...GeoParquet
}

