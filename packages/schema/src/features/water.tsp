import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("water")
@extension("additionalProperties", false)
model water {
  ...Topo.TopoIdentity;
  ...Topo.AuditFields;
  t50_fid?: unknown /* int64; hyparquet returns BigInt */;
  type?: "canal" | "lagoon" | "lake" | "pond" | "river" | null;
  water_use?: Topo.WaterUseEnum | null;
  hierarchy?: string | null;
  name?: string | null;
  group_name?: string | null;
  height?: float64 | null;
  perennial?: Topo.PerennialEnum | null;
  temperature?: Topo.TemperatureEnum | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
