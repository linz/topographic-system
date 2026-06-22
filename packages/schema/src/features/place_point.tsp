import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("place_point")
@extension("additionalProperties", false)
model place_point {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  type: "cave" | "cemetery" | "grave" | "historic_site" | "mine" | "monument" | "pa" | "radar_dome" | "saddle" | "satellite_station";
  place_type?: Topo.PlaceTypeEnum | null;
  status?: Topo.StatusEnum | null;
  name?: string | null;
  elevation?: int32 | null;
  composition?: Topo.CompositionEnum | null;
  description?: string | null;
  orientation?: float64 | null;
  substance_extracted?: Topo.SubstanceEnum | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}

