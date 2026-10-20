import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/nztopo50_grid")
@extension("additionalProperties", false)
model nztopo50_grid {
  /** UUIDv7 of the feature */
  id: string;

  created_at: utcDateTime;
  updated_at: utcDateTime;
  direction: grid_direction;

  /** Axis that `value` measures, in NZTM2000 (EPSG:2193) metres. */
  value_axis: grid_axis;

  value: float64;
  ...TopographicMetadata;
  ...GeoParquet;
}

/** Named for the axis the line runs along: an `easting` line holds a northing, and vice versa. */
enum grid_direction {
  Easting: "easting",
  Northing: "northing",
}

/** NZTM2000 (EPSG:2193) axis that a grid line's `value` measures. */
enum grid_axis {
  Easting: "easting",
  Northing: "northing",
}
