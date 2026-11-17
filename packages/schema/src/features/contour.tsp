import "@typespec/json-schema";
import "@typespec/versioning";
import "./common.tsp";

using TypeSpec.JsonSchema;
using TypeSpec.Versioning;

namespace Linz.Topography;

@jsonSchema("contour")
@extension("additionalProperties", false)
model contour {
  ...TopographicFeature;

  /**
   * Contour type, in order of precedence: `supplementary` (from the source `designated`),
   * `index` (elevation is a multiple of 100 m), `depression` (from the source `nat_form`),
   * otherwise `intermediate`.
   *
   * Changed from the single value `contour` in v0.4 (BM-1916).
   */
  @typeChangedFrom(Versions.v0_4, "contour")
  type: contour_type;

  /**
   * `depression` when the contour is a depression but another type took precedence.
   *
   * Added in v0.4 (BM-1916).
   */
  @added(Versions.v0_4)
  subtype: contour_subtype | null;

  elevation: int32 | null;

  /** Removed from v0.4 (BM-1916). */
  @removed(Versions.v0_4)
  definition: string | null;

  /** Removed from v0.4, now `type = supplementary` (BM-1916). */
  @removed(Versions.v0_4)
  designation: "supplementary" | null;

  /** Removed from v0.4, now `type` or `subtype` = `depression` (BM-1916). */
  @removed(Versions.v0_4)
  formation: "depression" | null;

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
