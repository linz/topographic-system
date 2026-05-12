import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("nz_topo50_dms_grid")
@extension("additionalProperties", false)
model nz_topo50_dms_grid {
  ...Topo.TopoIdentity;
  direction?: Topo.DirectionEnum | null;
  value?: float64 | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
