import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("structure")
@extension("additionalProperties", false)
model structure {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  type?: "dry_dock" | "fish_farm" | "marine_farm" | "reservoir" | "siphon" | "tank" | null;
  lid_type?: Topo.LidTypeEnum | null;
  subtype?: Topo.StructureSubTypeEnum | null;
  species?: Topo.SpeciesEnum | null;
  status?: Topo.StatusEnum | null;
  name?: string | null;
  stored_item?: Topo.StoredItemEnum | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
