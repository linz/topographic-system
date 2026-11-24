import "@typespec/json-schema";
import "./common.tsp";
using TypeSpec.JsonSchema;

namespace Linz.Topography;

@jsonSchema("land_cover_point")
@extension("additionalProperties", false)
model land_cover_point {
  ...TopographicFeature;
  type: land_cover_point_type | "rock_outcrop";
  orientation: int32 | null;
  elevation: int32 | null;
  subtype: rock_outcrop_subtype | null;
  name: string | null;
  theme: Theme.Landcover;
  ...TopographicMetadata;
  ...GeoParquet;
}

enum land_cover_point_type {
  Cemetery: "cemetery",
  Fumarole: "fumarole",
  Swamp: "swamp",
}

enum rock_outcrop_subtype {
  SmallRockOutcrop: "small_rock_outcrop",
  LargeRockOutcrop: "large_rock_outcrop",
  LargeBoulder: "large_boulder",
}
