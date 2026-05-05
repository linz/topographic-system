import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("nz_topo50_dms_grid.yaml")
@extension("additionalProperties", false)
model NzTopo50DmsGrid {
  ...Topo.TopoIdentity;
  direction?: Topo.DirectionEnum | null;
  value?: float64 | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
