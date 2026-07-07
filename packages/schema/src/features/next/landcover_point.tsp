 import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/landcover_point")
@extension("additionalProperties", false)
model landcover_point {
  ...TopographicFeature;

  type: landcover_point_type;
  subtype: landcover_point_subtype | null;
  name: string | null;
  orientation: float64 | null;
  elevation: int32 | null;

  ...GeoParquet
}

enum landcover_point_type {
  Cemetery: "cemetery";
  Fumarole: "fumarole";
  RockOutcrop: "rock_outcrop";
  Swamp: "swamp";
} 

enum landcover_point_subtype {
  SmallRockOutcrop: "small_rock_outcrop";
  LargeRockOutcrop: "large_rock_outcrop";
  LargeBoulder: "large_boulder";
}