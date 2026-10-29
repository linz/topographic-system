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
  epsg: int32;
  origin_x: float64;
  origin_y: float64;
  cover_front_origin_x: float64 | null;
  cover_front_origin_y: float64 | null;
  cover_back_origin_x: float64 | null;
  cover_back_origin_y: float64 | null;

  /**
   * id (UUID) of the example feature.
   *
   * Null where no feature matches the sheet's example name, e.g. a backfilled release that
   * predates the trig point or geographic name it names.
   */
  example_point_id: string | null;

  published_version: string;
  published_at: utcDateTime;
  barcode: string | null;
  created_at: utcDateTime;
  updated_at: utcDateTime;
  ...GeoParquet;
}
