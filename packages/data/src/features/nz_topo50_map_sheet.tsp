import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("nz_topo50_map_sheet.yaml")
@extension("additionalProperties", false)
model NzTopo50MapSheet {
  ...Topo.TopoIdentity;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  feature_type?: "nz_topo50_map_sheet" | null;
  sheet_code?: string | null;
  sheet_name?: string | null;
  edition?: string | null;
  example_class?: Topo.ExampleClassEnum | null;
  example_name?: string | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
