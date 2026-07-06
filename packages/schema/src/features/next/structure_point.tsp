import "@typespec/json-schema";
import "./common.tsp";
import "./structure.tsp"; 

using TypeSpec.JsonSchema;

@jsonSchema("next/structure_point")
@extension("additionalProperties", false)
model structure_point {
  ...TopographicFeature;

  type: structure_point_type;
  structure_use: structure_use | null;
  tank_type: "uncovered" | "water" | "surge chamber" | null;
  subtype: structure_subtype | null;
  status: structure_status | null;
  name: string | null;
  location: "land" | "lake" | "sea" | null;
  height: float64 | null;
  orientation: float64 | null;
  material: structure_material | null;
  restrictions: structure_restrictions | null;
  stored_item: structure_stored_item | null;
  wreck_of: "plane" | "ship" | null;

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
  Redoubt: "redoubt",
  Shaft: "shaft",
  Siphon: "siphon",
  Stockyard: "stockyard",
  Tank: "tank",
  Tower: "tower",
  Well: "well",
  Windmill: "windmill",
  Wreck: "wreck"
}


