import "@typespec/json-schema";
import "./common.tsp";
using TypeSpec.JsonSchema;

namespace Linz.Topography;

@jsonSchema("land_cover_point")
@oneOf
union land_cover_point {
  rock_outcrop: rock_outcrop,
  other_land_cover_points: land_cover_point_core_types,
}

// Not decorated with @jsonSchema so it does not get emitted standalone.
model LandCoverPointCommon {
  name: string | null;
  theme: Theme.Landcover;
  ...TopographicMetadata;
  ...GeoParquet;
}

@extension("additionalProperties", false)
model rock_outcrop {
  ...TopographicFeature;
  type: "rock_outcrop";
  orientation: float64;
  elevation: int32;
  subtype: rock_outcrop_subtype;
  ...LandCoverPointCommon;
}

@extension("additionalProperties", false)
model land_cover_point_core_types {
  ...TopographicFeature;
  type: land_cover_point_core_type;
  orientation: null;
  elevation: null;
  subtype: null;
  ...LandCoverPointCommon;
}

enum land_cover_point_core_type {
  Cemetery: "cemetery",
  Fumarole: "fumarole",
  Swamp: "swamp",
}

enum rock_outcrop_subtype {
  SmallRockOutcrop: "small_rock_outcrop",
  LargeRockOutcrop: "large_rock_outcrop",
  LargeBoulder: "large_boulder",
}
