import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/nztopo50_ice_contour")
@extension("additionalProperties", false)
model nztopo50_ice_contour {
  ...TopographicFeature;
  type: "contour_ice";
  elevation: int32 | null;
  definition: string | null;
  designation: "supplementary" | null;
  formation: "depression" | null;
  theme: Theme.Relief;

  /** UUID for the intersecting contour feature. */
  contour_id: string;

  /** UUID for the intersecting landcover feature. */
  landcover_id: string;

  ...TopographicMetadata;
  ...GeoParquet;
}
