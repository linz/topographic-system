import "@typespec/json-schema";
import "@typespec/versioning";
import "./common.tsp";

using TypeSpec.JsonSchema;
using TypeSpec.Versioning;

namespace Linz.Topography;

/**
 * 1:50,000 tile index: the NZTM2000 (EPSG:2193) Topo50 sheet grid used by the LINZ imagery and
 * mapping tile layouts. Imported from the LINZ Data Service.
 */
@jsonSchema("nztopo50_tile_index")
@extension("additionalProperties", false)
model nztopo50_tile_index {
  /** UUIDv7 of the feature */
  id: string;

  created_at: utcDateTime;
  updated_at: utcDateTime;

  /** Topo50 sheet code, e.g. `BM39`. */
  sheet_code_id: string;

  /** Map scale denominator. Always `50000` in this layer. */
  scale: int32;

  /** Whether a Topo50 map covers this sheet. `no` sheets have no published map. */
  topo50_coverage: topo50_coverage;

  ...TopographicMetadata;
  ...GeoParquet;
}

/** Whether a Topo50 map covers a 1:50k sheet. */
enum topo50_coverage {
  Yes: "yes",
  Partial: "partial",
  No: "no",
}
