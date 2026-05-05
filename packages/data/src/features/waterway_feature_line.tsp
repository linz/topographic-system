import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("waterway_feature_line.yaml")
@extension("additionalProperties", false)
model WaterwayFeatureLine {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  feature_type?: "rapid" | "waterfall" | "waterfall_edge" | null;
  name?: string | null;
  height?: float64 | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
