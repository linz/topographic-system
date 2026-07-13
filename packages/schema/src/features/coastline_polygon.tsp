import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

/**
 * Derived polygon Union of coastline and island
 */
@jsonSchema("coastline_polygon")
@extension("additionalProperties", false)
model coastline_polygon {
  /** UUID for the feature. */
  id?: string | null;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  type: "coastline" | "island";
  coastline_type?: string | null;
  elevation?: int32 | null;
  name?: string | null;
  group_name?: string | null;
  updated_at?: unknown;
  /** Default at write time: today. */
  created_at?: unknown;
  /** WKB polygon geometry in NZGD2000 (EPSG:4167). */
  @extension("not", #{ type: "null" })
  geometry: unknown;
}
