import "@typespec/json-schema";
import "./common.tsp";
using TypeSpec.JsonSchema;

@jsonSchema("next/landcover_point")
@oneOf
union landcover_point {
  rock_outcrop: rock_outcrop,
  other_landcover_points: landcover_point_core_types,
}

// Not decorated with @jsonSchema so it does not get emitted standalone.
model LandcoverPointCommon {
  name: string | null;
  orientation: float64 | null;
  elevation: int32 | null;
  theme: Theme.Landcover;
  ...TopographicMetadata;
  ...GeoParquet;
}

@extension("additionalProperties", false)
model rock_outcrop {
  ...TopographicFeature;
  type: "rock_outcrop";
  subtype: rock_outcrop_subtype;
  ...LandcoverPointCommon;
}

@extension("additionalProperties", false)
model landcover_point_core_types {
  ...TopographicFeature;
  type: landcover_point_core_type;
  subtype: null;
  ...LandcoverPointCommon;
}

enum landcover_point_core_type {
  Cemetery: "cemetery",
  Fumarole: "fumarole",
  Swamp: "swamp",
}

enum rock_outcrop_subtype {
  SmallRockOutcrop: "small_rock_outcrop",
  LargeRockOutcrop: "large_rock_outcrop",
  LargeBoulder: "large_boulder",
}
