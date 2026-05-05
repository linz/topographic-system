import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("relief_line.yaml")
@extension("additionalProperties", false)
model ReliefLine {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  feature_type?: "cliff_edge" | "cutting_edge" | "embankment" | "slip_edge" | null;
  relief_use?: Topo.ReliefUseEnum | null;
  name?: string | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
