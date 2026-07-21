import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/relief_point")
@extension("additionalProperties", false)
model relief_point {
  ...TopographicFeature;

  type: relief_point_type;
  name: string | null;
  elevation: int32 | null;
  height: float64 | null;
  orientation: float64 | null;

  ...TopographicMetadata;
  ...GeoParquet
}

enum relief_point_type {
  Height: "height",
  Saddle: "saddle",
  Sinkhole: "sinkhole",
  Waterfall: "waterfall"
}

