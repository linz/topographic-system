import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/landuse")
@extension("additionalProperties", false)
model landuse {
  ...TopographicFeature;
  type: landuse_type;
  subtype: landuse_subtype | null;
  status: landuse_status | null;
  name: string | null;
  substance_extracted: substance_extracted | null;

  ...GeoParquet
}

enum landuse_type {
  Cemetery: "cemetery",
  Cycle_track: "cycle_track",
  Dogtrack: "dog_track",
  GolfCourse: "golf_course",
  GravelPit: "gravel_pit",
  Horsetrack: "horse_track",
  Landfill: "landfill",
  Mine: "mine",
  Orchard: "orchard",
  PumicePit: "pumice_pit",
  Quarry: "quarry",
  Racetrack: "racetrack",
  RifleRange: "rifle_range",
  Showground: "showground",
  Sportsfield: "sportsfield",
  Vineyard: "vineyard",
  Vehicletrack: "vehicle_track",
}

enum landuse_subtype {
  Training: "training",
  Opencast: "opencast",
  Underground: "underground",
}

enum landuse_status {
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
  SilicaSand: "silica sand",
  Silver: "silver",
  Stone: "stone",
  Zeolite: "zeolite",
}