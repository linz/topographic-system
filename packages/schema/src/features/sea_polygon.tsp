import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

/**
 * Derived sea (moana) polygons for the water layer.
 *
 * The land polygons (coastline and island) are inverted and sliced by Web
 * Mercator quadkey tiles so no single large polygon exists.
 */
@jsonSchema("sea_polygon")
@extension("additionalProperties", false)
model sea_polygon {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;

  /** Web Mercator quadkey of the tile this sea polygon was sliced to. */
  quadkey: string;

  type: "moana";

  /** WKB polygon geometry in NZGD2000 (EPSG:4167). */
  @extension("not", #{ type: "null" })
  geometry: unknown;
}
