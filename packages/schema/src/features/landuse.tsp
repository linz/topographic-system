import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("landuse")
@extension("additionalProperties", false)
model landuse {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  type?: "cemetery" | "golf_course" | "gravel_pit" | "landfill" | "mine" | "pumice_pit" | "quarry" | "racetrack" | "rifle_range" | "showground" | "sportsfield" | null;
  landuse_use?: Topo.LanduseUseEnum | null;
  subtype?: Topo.LanduseSubTypeEnum | null;
  status?: Topo.StatusEnum | null;
  name?: string | null;
  substance?: Topo.SubstanceEnum | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
