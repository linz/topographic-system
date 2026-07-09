import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/landuse")
@extension("additionalProperties", false)
model landuse {
  ...TopographicFeature;
  type: landuse_type;
  landuse_use: "horse" | "vehicle" | null;
  subtype: "training" | null;
  status: landuse_status | null;
  name: string | null;
  substance_extracted: substance_extracted | null;

  ...GeoParquet
}

enum landuse_type {
  Cemetery: "cemetery";
  GolfCourse: "golf_course";
  GravelPit: "gravel_pit";
  Landfill: "landfill";
  Mine: "mine";
  Orchard: "orchard";
  PumicePit: "pumice_pit";
  Quarry: "quarry";
  Racetrack: "racetrack";
  RifleRange: "rifle_range";
  Showground: "showground";
  Sportsfield: "sportsfield";
  Vineyard: "vineyard";
}

enum landuse_status {
  Dangerous: "dangerous",
  Disused: "disused",
  Historic: "historic",
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