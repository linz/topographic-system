import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/railway_station")
@extension("additionalProperties", false)
model railway_station {
  ...TopographicFeature;

  type: "rail_station";
  name: string | null;
  
  ...TopographicMetadata;
  ...GeoParquet
}

