import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/nztopo50_grid")
@extension("additionalProperties", false)
model nztopo50_grid {
  ...Topo.ProductIdentity;
  direction: Topo.DirectionEnum | null;
  value: float64 | null;
  ...TopographicMetadata;
  ...GeoParquet
}
