import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("runway.yaml")
@extension("additionalProperties", false)
model Runway {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  feature_type?: "runway" | null;
  runway_use?: Topo.RunwayUseEnum | null;
  status?: Topo.StatusEnum | null;
  surface?: Topo.SurfaceEnum | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
