import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/nztopo50_carto_symbol")
@extension("additionalProperties", false)
model nztopo50_carto_symbol {
  ...ProductFeature;
  created_at: utcDateTime;
  updated_at: utcDateTime;

  /**
   * id (UUID) of the feature this symbol annotates: the landuse polygon it sits within, or
   * the road centreline it labels.
   *
   * Null where no feature was close enough to match.
   */
  source_id: string | null;

  type: carto_symbol_type;
  label: string | null;

  /** Label rotation in degrees clockwise from east. Null for symbols drawn unrotated. */
  orientation: int32 | null;

  ...GeoParquet;
}

enum carto_symbol_type {
  ContourNumber: "contour_number",
  GolfCourse: "golf_course",
  HighwayShield: "highway_shield",
  MineOpencast: "mine_opencast",
  MineTraining: "mine_training",
  MineUnderground: "mine_underground",
}
