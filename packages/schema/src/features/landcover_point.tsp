import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("landcover_point")
@extension("additionalProperties", false)
model landcover_point {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  type?: "fumarole" | "rock_outcrop" | null;
  name?: string | null;
  elevation?: int32 | null;
  display?: Topo.DisplayEnum | null;
  orientation?: float64 | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
