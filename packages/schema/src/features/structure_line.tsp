import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("structure_line")
@extension("additionalProperties", false)
model structure_line {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  type: "boatramp" | "boom" | "breakwater" | "cableway_industrial" | "cableway_people" | "dam" | "flume" | "ladder" | "marine_farm" | "ski_lift" | "ski_tow" | "slipway" | "spillway_edge" | "walkwire" | "water_race" | "weir" | "wharf" | "wharf_edge";
  structure_use?: Topo.StructureUseEnum | null;
  species?: Topo.SpeciesEnum | null;
  status?: Topo.StatusEnum | null;
  name?: string | null;
  material?: Topo.MaterialEnum | null;
  material_conveyed?: Topo.MaterialConveyedEnum | null;
  restrictions?: Topo.RestrictionsEnum | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}

