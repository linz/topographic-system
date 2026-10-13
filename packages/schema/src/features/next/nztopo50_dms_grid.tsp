import "./nztopo50_grid.tsp";
using TypeSpec.JsonSchema;

@jsonSchema("next/nztopo50_dms_grid")
@extension("additionalProperties", false)
model nztopo50_dms_grid {
  ...ProductFeature;
  direction: dms_direction;
  value: float64;
  ...GeoParquet;
}

/** Named for the axis the line runs along: a `longitude` line holds a latitude, and vice versa. */
enum dms_direction {
  Latitude: "latitude",
  Longitude: "longitude",
}
