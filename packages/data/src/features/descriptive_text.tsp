import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("descriptive_text.yaml")
@extension("additionalProperties", false)
model DescriptiveText {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  feature_type?: "descriptive_text" | null;
  nzgb_id?: unknown /* int64; hyparquet returns BigInt */;
  info_display?: string | null;
  size?: float64 | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
