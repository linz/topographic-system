import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;
using TypeSpec.Versioning;

namespace Linz.Topography;

@jsonSchema("nztopo50_grid")
@extension("additionalProperties", false)
model nztopo50_grid {
  /** UUIDv7 of the feature */
  id: string;

  /**
   * Reference topo50 feature ID.
   *
   * Removed from v0.4 (BM-1908).
   */
  @removed(Versions.v0_4)
  t50_fid: uint32 | null;

  @added(Versions.v0_4)
  created_at: utcDateTime;

  @added(Versions.v0_4)
  updated_at: utcDateTime;

  /** The axis the line runs along. An `easting` line runs east–west, so its `value` is a northing. */
  direction: grid_direction;

  /** Position of the line in NZTM2000 (EPSG:2193) metres: a northing for an `easting` line, an easting for a `northing` line. */
  value: float64;

  @added(Versions.v0_4)
  metadata: string | null;

  ...GeoParquet;
}

/** An NZTM2000 (EPSG:2193) grid axis. */
enum grid_direction {
  Easting: "easting",
  Northing: "northing",
}
