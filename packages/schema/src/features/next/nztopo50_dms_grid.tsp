import "./nztopo50_grid.tsp";
using TypeSpec.JsonSchema;

@jsonSchema("next/nztopo50_dms_grid")
@extension("additionalProperties", false)
model nztopo50_dms_grid {
  ...ProductFeature;
  created_at: utcDateTime;
  updated_at: utcDateTime;
  direction: dms_direction;

  /** Axis that `value` measures, in WGS84 (EPSG:4326) decimal degrees. */
  value_axis: dms_axis;

  value: float64;
  ...TopographicMetadata;
  ...GeoParquet;
}

/** Named for the axis the line runs along: a `longitude` line holds a latitude, and vice versa. */
enum dms_direction {
  Latitude: "latitude",
  Longitude: "longitude",
}

/** WGS84 (EPSG:4326) axis that a DMS grid line's `value` measures. */
enum dms_axis {
  Latitude: "latitude",
  Longitude: "longitude",
}
