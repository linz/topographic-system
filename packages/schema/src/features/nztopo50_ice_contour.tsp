import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("nztopo50_ice_contour")
@extension("additionalProperties", false)
model nztopo50_ice_contour {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  type: "contour";
  elevation?: int32 | null;
  definition?: string | null;
  designation?: Topo.DesignationEnum | null;
  formation?: Topo.FormationEnum | null;
  /** UUID for the intersecting landcover feature. */
  landcover_id: string;
  landcover_type: "ice";
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
