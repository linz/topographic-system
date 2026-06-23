import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("relief_line")
@extension("additionalProperties", false)
model relief_line {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  type: "cliff_edge" | "cutting_edge" | "embankment" | "rapid" | "slip_edge" | "waterfall" | "waterfall_edge";
  relief_use?: Topo.ReliefUseEnum | null;
  name?: string | null;
  height?: float64 | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}

