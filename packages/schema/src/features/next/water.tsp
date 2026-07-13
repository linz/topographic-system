import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/water")
@extension("additionalProperties", false)
model water {
  ...TopographicFeature;

  type: "canal" | "lagoon" | "lake" | "pond" | "river";
  water_use: water_use | null;
  hierarchy: string | null;
  name: string | null;
  group_name: string | null;
  height: float64 | null;
  elevation: int32 | null;
  perennial: water_perennial | null;
  temperature_indicator: water_temperature_indicator | null;

  ...TopographicMetadata;
  ...GeoParquet
}

enum water_use {
  Evaporation: "evaporation",
  HydroElectric: "hydro_electric",
  IceSkating: "ice_skating",
  Oil: "oil",
  Oxidation: "oxidation",
  Reservoir: "reservoir",
  Settling: "settling",
  Sewage: "sewage",
  SewageTreatment: "sewage_treatment",
  Sludge: "sludge",
}

enum water_perennial {
  Dry: "dry",
  Seasonal: "seasonal",
}

enum water_temperature_indicator {
  Cold: "cold",
  Hot: "hot",
}
