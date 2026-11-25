import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

namespace Linz.Topography;

@jsonSchema("contour")
@extension("additionalProperties", false)
model contour {
  ...TopographicFeature;

  /**
   * Contour type, in order of precedence: `supplementary` (from the source `designated`),
   * `index` (elevation is a multiple of 100 m), `depression` (from the source `nat_form`),
   * otherwise `intermediate`.
   */
  type: contour_type;

  /**
   * `depression` when the contour is a depression but another type took precedence.
   */
  subtype: contour_subtype | null;

  elevation: int32 | null;
  theme: Theme.Relief;
  ...TopographicMetadata;
  ...GeoParquet;
}

enum contour_type {
  Intermediate: "intermediate",
  Index: "index",
  Supplementary: "supplementary",
  Depression: "depression",
}

enum contour_subtype {
  Depression: "depression",
}
