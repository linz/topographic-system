import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/runway")
@extension("additionalProperties", false)
model runway {
  ...TopographicFeature;

  type: "runway";
  subtype: runway_subtype | null;
  status: runway_status | null;
  surface: runway_surface | null;
  
  ...TopographicMetadata;
  ...GeoParquet
}

enum runway_subtype {
  Aerodrome: "aerodrome",
  Airport: "airport",
  Airstrip: "airstrip",
}

enum runway_status {
  Disused: "disused",
}

enum runway_surface {
  Grass: "grass",
  Sealed: "sealed",
}