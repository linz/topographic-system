import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("tunnel_line.yaml")
@extension("additionalProperties", false)
model TunnelLine {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  feature_type?: "tunnel" | null;
  tunnel_use?: Topo.TunnelUseEnum | null;
  tunnel_use2?: Topo.TunnelUse2Enum | null;
  tunnel_type?: Topo.TunnelTypeEnum | null;
  status?: Topo.StatusEnum | null;
  name?: string | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
