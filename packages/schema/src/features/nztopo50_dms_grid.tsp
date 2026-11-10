import "./nztopo50_grid.tsp";
using TypeSpec.JsonSchema;
using TypeSpec.Versioning;

namespace Linz.Topography;

@jsonSchema("nztopo50_dms_grid")
@extension("additionalProperties", false)
model nztopo50_dms_grid {
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

  direction: dms_direction;

  /** Axis that `value` measures, in WGS84 (EPSG:4326) decimal degrees. */
  @added(Versions.v0_4)
  value_axis: dms_axis;

  value: float64;

  @added(Versions.v0_4)
  metadata: string | null;

  ...GeoParquet;
}

/** Named for the axis the line runs along: a `longitude` line holds a latitude, and vice versa. */
enum dms_direction {
  Latitude: "latitude",
  Longitude: "longitude",
}

/** WGS84 (EPSG:4326) axis that a DMS grid line's `value` measures. */
@added(Versions.v0_4)
enum dms_axis {
  Latitude: "latitude",
  Longitude: "longitude",
}
