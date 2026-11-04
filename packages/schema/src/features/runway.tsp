import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

namespace Linz.Topography;

@jsonSchema("runway")
@extension("additionalProperties", false)
model runway {
  ...TopographicFeature;
  type: runway_type | null;
  subtype: runway_subtype | null;
  status: runway_status | null;
  theme: Theme.Transport;
  ...TopographicMetadata;
  ...GeoParquet;
}

enum runway_type {
  Aerodrome: "aerodrome",
  Airport: "airport",
  Airstrip: "airstrip",
}

enum runway_subtype {
  Grass: "grass",
  Sealed: "sealed",
}

enum runway_status {
  Disused: "disused",
}
