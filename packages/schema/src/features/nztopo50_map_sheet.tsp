import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("nz_topo50_map_sheet")
@extension("additionalProperties", false)
model nz_topo50_map_sheet {
  ...Topo.TopoIdentity;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  type?: "nztopo50_map_sheet" | null;
  sheet_code?: string | null;
  sheet_name?: string | null;
  x_origin?: float64 | null;
  y_origin?: float64 | null;
  /** topo_id (UUID) of the example feature. */
  example_point_id?: string | null;
  published_version?: string | null;
  published_at: {};
  updated_at: {};
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
