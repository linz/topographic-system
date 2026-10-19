import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;
using TypeSpec.Versioning;

namespace Linz.Topography;

@jsonSchema("nztopo50_grid")
@extension("additionalProperties", false)
model nztopo50_grid {
  ...ProductFeature;

  @added(Versions.v0_4)
  created_at: utcDateTime;

  @added(Versions.v0_4)
  updated_at: utcDateTime;

  direction: grid_direction;

  /** Axis that `value` measures, in NZTM2000 (EPSG:2193) metres. */
  @added(Versions.v0_4)
  value_axis: grid_axis;

  value: float64;

  @added(Versions.v0_4)
  metadata: string | null;

  ...GeoParquet;
}

enum grid_direction {
  Easting: "easting",
  Northing: "northing",
}

/** NZTM2000 (EPSG:2193) axis that a grid line's `value` measures. */
@added(Versions.v0_4)
enum grid_axis {
  Easting: "easting",
  Northing: "northing",
}
