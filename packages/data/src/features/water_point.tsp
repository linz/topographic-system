import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("water_point.yaml")
@extension("additionalProperties", false)
model WaterPoint {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  feature_type?: "rock" | "soakhole" | "spring" | "swamp" | "waterfall" | null;
  name?: string | null;
  height?: float64 | null;
  orientation?: float64 | null;
  temperature_indicator?: Topo.TemperatureIndicatorEnum | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
