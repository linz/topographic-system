import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/road_line")
@extension("additionalProperties", false)
model road_line {
  ...TopographicFeature;
  
  type: "road";
  hierarchy: string | null;
  status: "closed" | "under_construction" | null;
  name: string | null;
  highway_number: string | null;
  lane_count: int32 | null;
  surface: "metalled" | "sealed" | "unmetalled" | null;
  way_count: "one_way" | null;
  width_indicator: "w" | null;

  ...TopographicMetadata;
  ...GeoParquet
}

