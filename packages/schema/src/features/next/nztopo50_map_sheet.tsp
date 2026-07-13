import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/nztopo50_map_sheet")
@extension("additionalProperties", false)
model nztopo50_map_sheet {
  ...ProductFeature;

  type: "nztopo50_map_sheet";
  sheet_code: string;
  sheet_name: string;
  origin_x: float64;
  origin_y: float64;
  /** id (UUID) of the example feature. */
  example_point_id: string;
  published_version: string;
  published_at: utcDateTime;
  updated_at: utcDateTime;

  ...GeoParquet
}
