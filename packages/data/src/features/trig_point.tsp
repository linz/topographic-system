import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("trig_point.yaml")
@extension("additionalProperties", false)
model TrigPoint {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  feature_type?: "trig" | null;
  trig_type?: Topo.TrigTypeEnum | null;
  name?: string | null;
  code?: string | null;
  elevation?: int32 | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
