import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("railway_line")
@extension("additionalProperties", false)
model railway_line {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  type: "railway";
  railway_use?: Topo.RailwayUseEnum | null;
  track_type?: Topo.TrackTypeEnum | null;
  vehicle_type?: Topo.VehicleTypeEnum | null;
  status?: Topo.StatusEnum | null;
  name?: string | null;
  nzgb_id?: unknown /* int64; hyparquet returns BigInt */;
  route?: string | null;
  route2?: string | null;
  route3?: string | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}

