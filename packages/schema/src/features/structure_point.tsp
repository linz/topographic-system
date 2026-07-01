import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("structure_point")
@extension("additionalProperties", false)
model structure_point {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  type: "beacon" | "bivouac" | "buoy" | "chimney" | "dredge" | "flare" | "floodgate" | "gate" | "ladder" | "mast" | "redoubt" | "shaft" | "siphon" | "stockyard" | "tank" | "tower" | "well" | "windmill" | "wreck";
  structure_use?: Topo.StructureUseEnum | null;
  tank_type?: "uncovered" | "water" | "surge chamber" | null;
  subtype?: Topo.StructureSubTypeEnum | null;
  status?: Topo.StatusEnum | null;
  name?: string | null;
  location?: Topo.LocationEnum | null;
  height?: float64 | null;
  orientation?: float64 | null;
  material?: Topo.MaterialEnum | null;
  restrictions?: Topo.RestrictionsEnum | null;
  stored_item?: Topo.StoredItemEnum | null;
  wreck_of?: Topo.WreckOfEnum | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}

