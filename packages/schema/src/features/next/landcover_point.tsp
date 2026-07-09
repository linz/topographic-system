 import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/landcover_point")
@extension("additionalProperties", false)
@jsonSchema("landcover_point")
@oneOf
union landcover_point {
  rock_outcrop: rock_outcrop,
  other_landcover_points: landcover_point_core_types,
}

model rock_outcrop {
    ...TopographicFeature;
    type: "rock_outcrop";
    subtype: rock_outcrop_subtype;
    ...GeoParquet
}

model landcover_point_core_types {
  ...TopographicFeature;
  type: landcover_point_core_type;
  subtype: null;
  ...GeoParquet
}

enum landcover_point_core_type {
  Cemetery: "cemetery",
  Fumarole: "fumarole",
  Swamp: "swamp"
} 

enum rock_outcrop_subtype {
  SmallRockOutcrop: "small_rock_outcrop",
  LargeRockOutcrop: "large_rock_outcrop",
  LargeBoulder: "large_boulder"
}