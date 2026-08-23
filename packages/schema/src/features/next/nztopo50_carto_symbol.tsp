import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/nztopo50_carto_symbol")
@extension("additionalProperties", false)
model nztopo50_carto_symbol {
  ...ProductFeature;
  created_at: utcDateTime;
  updated_at: utcDateTime;
  type: carto_symbol_type;
  label: string | null;
  orientation: float64 | null;
  ...GeoParquet;
}

enum carto_symbol_type {
  GolfCourse: "golf_course",
}
