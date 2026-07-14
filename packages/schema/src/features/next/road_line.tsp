import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/road_line")
@extension("additionalProperties", false)
model road_line {
  ...TopographicFeature;

  type: "road";
  hierarchy: string | null;
  status: road_status | null;
  name: string | null;
  highway_number: string | null;
  lane_count: int32 | null;
  surface: road_surface | null;
  way_count: "one_way" | null;
  width_indicator: "w" | null;
  road_access: "mp" | null;
  rna_sufi: unknown /* int64; hyparquet returns BigInt */;

  ...GeoParquet
}

enum road_status {
  UnderConstruction: "under_construction",
  Closed: "closed"
}

enum road_surface {
  Metalled: "metalled",
  Unmetalled: "unmetalled",
  Sealed: "sealed",
}