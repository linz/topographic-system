import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("tunnel_line")
@extension("additionalProperties", false)
model tunnel_line {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  type: "tunnel";
  tunnel_use?: Topo.TunnelUseEnum | null;
  tunnel_use2?: Topo.TunnelUse2Enum | null;
  subtype?: Topo.TunnelSubTypeEnum | null;
  status?: Topo.StatusEnum | null;
  name?: string | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}

