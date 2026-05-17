import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("contour")
@extension("additionalProperties", false)
model contour {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: int32 | null;
  feature_type?: "contour" | null;
  elevation?: int32 | null;
  definition?: string | null;
  designation?: Topo.DesignationEnum | null;
  formation?: Topo.FormationEnum | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
