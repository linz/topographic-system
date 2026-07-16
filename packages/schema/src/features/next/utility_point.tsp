import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/utility_point")
@extension("additionalProperties", false)
model utility_point {
  ...TopographicFeature;

  type: utility_point_type;
  name: string | null;
  orientation: float64 | null;

  ...TopographicMetadata;
  ...GeoParquet
}

enum utility_point_type {
  DryDock: "dry_dock",
  FishFarm: "fish_farm",
  MarineFarm: "marine_farm",
  Reservoir: "reservoir",
  Siphon: "siphon",
  Tank: "tank"
}
