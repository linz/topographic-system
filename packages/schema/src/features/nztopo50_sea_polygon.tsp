import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

/**
 * Derived sea (moana) polygons for the water layer.
 *
 * The land polygons (coastline and island) are inverted and sliced by Web
 * Mercator quadkey tiles so no single large polygon exists.
 */
@jsonSchema("next/sea_polygon")
@extension("additionalProperties", false)
model sea_polygon {
  id: string;
  type: "moana";

  /** Web Mercator quadkey of the tile this sea polygon was sliced to. */
  quadkey: string;

  created_at: utcDateTime;
  updated_at: utcDateTime;
  ...GeoParquet;
}
