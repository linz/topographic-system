import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("bridge_line.yaml")
@extension("additionalProperties", false)
model BridgeLine {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  feature_type?: "bridge" | null;
  bridge_use?: Topo.BridgeUseEnum | null;
  bridge_use2?: Topo.BridgeUse2Enum | null;
  construction_type?: Topo.ConstructionTypeEnum | null;
  status?: Topo.StatusEnum | null;
  name?: string | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
