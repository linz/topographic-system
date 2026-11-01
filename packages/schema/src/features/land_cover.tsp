import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("land_cover")
@extension("additionalProperties", false)
model land_cover {
  ...TopographicFeature;
  type: land_cover_type;
  name: string | null;
  theme: Theme.Landcover;
  ...TopographicMetadata;
  ...GeoParquet;
}

enum land_cover_type {
  Ice: "ice",
  Moraine: "moraine",
  MoraineWall: "moraine_wall",
  Mud: "mud",
  Sand: "sand",
  Scree: "scree",
  Shingle: "shingle",
  Swamp: "swamp",
}
