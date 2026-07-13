import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/building_point")
@extension("additionalProperties", false)
model building_point {
  ...TopographicFeature;

  type: "building";
  building_use: string | null;
  status: Topo.StatusEnum | null;
  name: string | null;
  orientation: float64 | null;
  
  ...TopographicMetadata;
  ...GeoParquet
}

