import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("landuse_line.yaml")
@extension("additionalProperties", false)
model LanduseLine {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  feature_type?: "racetrack" | null;
  landuse_use?: Topo.LanduseUseEnum | null;
  landuse_type?: Topo.LanduseTypeEnum | null;
  name?: string | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
