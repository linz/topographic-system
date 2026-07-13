import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/nztopo50_map_sheet")
@extension("additionalProperties", false)
model nztopo50_map_sheet {
  ...Topo.ProductIdentity;
  t50_fid: unknown /* int64; hyparquet returns BigInt */;
  type: "nztopo50_map_sheet";
  sheet_code: string;
  sheet_name: string;
  x_origin: float64;
  y_origin: float64;
  /** topo_id (UUID) of the example feature. */
  example_point_id: string;
  published_version: string;
  published_at: {};
  updated_at: {};
  ...TopographicMetadata;
  ...GeoParquet
}
