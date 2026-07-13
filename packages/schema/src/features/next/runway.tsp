import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/runway")
@extension("additionalProperties", false)
model runway {
  ...TopographicFeature;

  type: "runway";
  runway_use: Topo.RunwayUseEnum | null;
  status: Topo.StatusEnum | null;
  surface: Topo.SurfaceEnum | null;
  
  ...TopographicMetadata;
  ...GeoParquet
}

