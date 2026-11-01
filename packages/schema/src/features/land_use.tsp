import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("land_use")
@extension("additionalProperties", false)
model land_use {
  ...TopographicFeature;
  type: land_use_type;
  subtype: land_use_subtype | null;
  status: land_use_status | null;
  name: string | null;
  substance_extracted: substance_extracted | null;
  theme: Theme.Landuse;
  ...TopographicMetadata;
  ...GeoParquet;
}

enum land_use_type {
  Cemetery: "cemetery",
  CycleRacetrack: "cycle_racetrack",
  DogRacetrack: "dog_racetrack",
  GolfCourse: "golf_course",
  GravelPit: "gravel_pit",
  HorseRacetrack: "horse_racetrack",
  Landfill: "landfill",
  Mine: "mine",
  Orchard: "orchard",
  PumicePit: "pumice_pit",
  Quarry: "quarry",
  Racetrack: "racetrack",
  ResidentialArea: "residential_area",
  RestrictedArea: "restricted_area",
  RifleRange: "rifle_range",
  Showground: "showground",
  Sportsfield: "sportsfield",
  Vineyard: "vineyard",
  VehicleRacetrack: "vehicle_racetrack",
}

enum land_use_subtype {
  Training: "training",
  Opencast: "opencast",
  Underground: "underground",
}

enum land_use_status {
  Dangerous: "dangerous",
  Disused: "disused",
  Old: "old",
}

enum substance_extracted {
  Bentonite: "bentonite",
  Clay: "clay",
  Coal: "coal",
  Gold: "gold",
  Gravel: "gravel",
  Ironsand: "ironsand",
  Lime: "lime",
  Limestone: "limestone",
  Metal: "metal",
  Quartz: "quartz",
  Scheelite: "scheelite",
  Shingle: "shingle",
  SilicaSand: "silica_sand",
  Silver: "silver",
  Stone: "stone",
  Zeolite: "zeolite",
}
