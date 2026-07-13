import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/building")
@extension("additionalProperties", false)
model building {
  ...TopographicFeature;

  type: "building";
  building_use: string | null;
  status: Topo.StatusEnum | null;
  name: string | null;
  
  ...TopographicMetadata;
  ...GeoParquet
}

