import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

/**
 * Derived polygon Union of coastline and island
 */
@jsonSchema("next/coastline_polygon")
@extension("additionalProperties", false)
model coastline_polygon {
  ...TopographicFeature;
  type: "coastline" | "island";
  elevation: int32 | null;
  name: string | null;
  group_name: string | null;
  ...GeoParquet;
}
