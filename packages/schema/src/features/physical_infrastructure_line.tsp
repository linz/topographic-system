import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("physical_infrastructure_line")
@extension("additionalProperties", false)
model physical_infrastructure_line {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  feature_type?: "pipeline" | "powerline" | "telephone" | "walkwire" | "water_race" | null;
  infrastructure_use?: Topo.InfrastructureUseEnum | null;
  support_type?: Topo.SupportTypeEnum | null;
  status?: Topo.StatusEnum | null;
  name?: string | null;
  visibility?: Topo.VisibilityEnum | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
