import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/track_line")
@extension("additionalProperties", false)
model track_line {
  ...TopographicFeature;

  type: "track";
  track_use: Topo.TrackUseEnum | null;
  track_type: Topo.TrackTypeEnum | null;
  status: Topo.StatusEnum | null;
  name: string | null;
  
  ...TopographicMetadata;
  ...GeoParquet
}

