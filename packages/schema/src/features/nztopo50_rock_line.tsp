import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("nztopo50_rock_line")
@extension("additionalProperties", false)
model nztopo50_rock_line {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  /** UUID of the source `marine` rock polygon this boundary line was derived from. */
  marine_id: string;
  type: "rock";
  name?: string | null;
  composition?: Topo.CompositionEnum | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
