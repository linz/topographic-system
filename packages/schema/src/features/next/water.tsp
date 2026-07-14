import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/water")
@extension("additionalProperties", false)
model water {
  ...TopographicFeature;

  type: water_type;
  subtype: water_subtype | null;
  hierarchy: string | null;
  name: string | null;
  group_name: string | null;
  nzgb_feat_id: unknown /* int64; hyparquet returns BigInt */;
  height: float64 | null;
  elevation: int32 | null;
  perennial: water_perennial | null;
  temperature_indicator: water_temperature_indicator | null;

  ...GeoParquet
}

enum water_type {
  Canal: "canal",
  Drain: "drain",
  Lagoon: "lagoon",
  Lake: "lake",
  Pond: "pond",
  River: "river",
  Soakhole: "soakhole",
  Spring: "spring",
  Waterfall: "waterfall",
}


enum water_subtype {
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
