import "@typespec/json-schema";
import "./common.tsp";
import "./building.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/building_point")
@extension("additionalProperties", false)
model building_point {
  ...TopographicFeature;

  type: "building";
  subtype: building_subtype | null;
  status: building_status | null;
  name: string | null;
  orientation: float64 | null;
  
  theme: "buildings";
  ...TopographicMetadata;
  ...GeoParquet
}


