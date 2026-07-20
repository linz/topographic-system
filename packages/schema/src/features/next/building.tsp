import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/building")
@extension("additionalProperties", false)
model building {
  ...TopographicFeature;

  type: "building";
  subtype: building_subtype | null;
  status: building_status | null;
  name: string | null;
  
  ...TopographicMetadata;
  ...GeoParquet
}

enum building_subtype {
  Abattoir: "abattoir",
  Camp: "camp",
  CementWorks: "cement_works",
  Church: "church",
  EnergyFacility: "energy_facility",
  Factory: "factory",
  FertiliserWorks: "fertiliser_works",
  FireLookout: "fire_lookout",
  ForestHeadquarters: "forest_headquarters",
  GasCompound: "gas_compound",
  Greenhouse: "greenhouse",
  GunClub: "gun_club",
  GunEmplacement: "gun_emplacement",
  Hall: "hall",
  Homestead: "homestead",
  Hospital: "hospital",
  Hut: "hut",
  Lodge: "lodge",
  Marae: "marae",
  Mill: "mill",
  Museum: "museum",
  Observatory: "observatory",
  Polytechnic: "polytechnic",
  PowerGeneration: "power_generation",
  Prison: "prison",
  PrivateHut: "private_hut",
  SaltWorks: "salt_works",
  School: "school",
  ShingleWorks: "shingle_works",
  Shelter: "shelter",
  Silo: "silo",
  StampingBattery: "stamping_battery",
  Substation: "substation",
  SurfClub: "surf_club",
  University: "university",
  VisitorCentre: "visitor_centre",
  WaterTreatmentPlant: "water_treatment_plant"
}

enum building_status {
  Derelict: "derelict",
  Historic: "historic",
  Private: "private",
}
