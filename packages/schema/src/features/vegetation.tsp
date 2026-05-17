import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("vegetation")
@extension("additionalProperties", false)
model vegetation {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  feature_type?: "exotic" | "native" | "orchard" | "scattered_scrub" | "scrub" | "vineyard" | null;
  sub_type?: Topo.SubTypeEnum | null;
  species?: Topo.SpeciesEnum | null;
  name?: string | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
