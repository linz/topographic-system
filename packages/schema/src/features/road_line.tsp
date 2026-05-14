import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("road_line")
@extension("additionalProperties", false)
model road_line {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  feature_type?: "road" | null;
  hierarchy?: string | null;
  status?: Topo.StatusEnum | null;
  name?: string | null;
  highway_number?: string | null;
  lane_count?: int32 | null;
  surface?: Topo.SurfaceEnum | null;
  way_count?: Topo.WayCountEnum | null;
  road_access?: Topo.RoadAccessEnum | null;
  name_id?: unknown /* int64; hyparquet returns BigInt */;
  rna_sufi?: unknown /* int64; hyparquet returns BigInt */;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
