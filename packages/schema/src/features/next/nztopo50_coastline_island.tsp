import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

/**
 * Derived polygon Union of coastline and island
 */
@jsonSchema("next/nztopo50_coastline_island")
@extension("additionalProperties", false)
model nztopo50_coastline_island {
  ...TopographicFeature;
  type: "coastline" | "island";
  elevation: int32 | null;
  name: string | null;
  group_name: string | null;
  ...GeoParquet;
}
