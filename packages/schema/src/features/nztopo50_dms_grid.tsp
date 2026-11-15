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

  /**
   * The axis the line runs along. A `longitude` line runs east–west (a parallel), so its `value` is a latitude.
   * Note this is the reverse of the usual "line of longitude", which means a meridian.
   */
  direction: dms_direction;

  /**
   * The axis `value` is measured on: `latitude` for an east–west line, `longitude` for a north–south line.
   * Always the opposite of `direction`.
   */
  @added(Versions.v0_4)
  value_axis: dms_direction;

  /** Position of the line in WGS84 (EPSG:4326) decimal degrees: a latitude for a `longitude` line, a longitude for a `latitude` line. */
  value: float64;

  @added(Versions.v0_4)
  metadata: string | null;

  ...GeoParquet;
}

/** A WGS84 (EPSG:4326) axis. */
enum dms_direction {
  Latitude: "latitude",
  Longitude: "longitude",
}
