import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("railway_station.yaml")
@extension("additionalProperties", false)
model RailwayStation {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  feature_type?: "rail_station" | null;
  name?: string | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
