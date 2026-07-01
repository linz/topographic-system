import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("nztopo50_dms_grid")
@extension("additionalProperties", false)
model nztopo50_dms_grid {
  ...Topo.ProductIdentity;
  direction?: Topo.DirectionEnum | null;
  value?: float64 | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
