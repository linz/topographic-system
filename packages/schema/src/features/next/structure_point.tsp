import "@typespec/json-schema";
import "./common.tsp";
import "./structure.tsp"; 

using TypeSpec.JsonSchema;

@jsonSchema("next/structure_point")
@extension("additionalProperties", false)
model structure_point {
  ...TopographicFeature;

  type: structure_point_type;
  subtype: structure_subtype | null;
  tank_type: structure_tank_type | null;
  status: structure_status | null;
  name: string | null;
  location: "land" | "lake" | "sea" | null;
  height: float64 | null;
  orientation: float64 | null;

  ...GeoParquet
}

enum structure_point_type {
  Beacon: "beacon",
  Bivouac: "bivouac",
  Buoy: "buoy",
  Chimney: "chimney",
  Dredge: "dredge",
  Flare: "flare",
  Floodgate: "floodgate",
  Gate: "gate",
  Ladder: "ladder",
  Mast: "mast",
  RadarDome: "radar_dome",  
  Redoubt: "redoubt",
  SatelliteStation: "satellite_station",
  Shaft: "shaft",
  Siphon: "siphon",
  Stockyard: "stockyard",
  Tank: "tank",
  Tower: "tower",
  Well: "well",
  Windmill: "windmill",
  Wreck: "wreck",
}


