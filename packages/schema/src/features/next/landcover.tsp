import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/landcover")
@extension("additionalProperties", false)
model landcover {
  ...TopographicFeature;

  type: landcover_type;
  name: string | null;
 
  ...TopographicMetadata;
  ...GeoParquet
}

enum landcover_type {
  Ice: "ice",
  Moraine: "moraine",
  MoraineWall: "moraine_wall",
  Mud: "mud",
  Sand: "sand",
  Scree: "scree",
  Shingle: "shingle",
  Swamp: "swamp"
}

