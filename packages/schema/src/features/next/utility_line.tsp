import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/utility_line")
@extension("additionalProperties", false)
model utility_line {
  ...TopographicFeature;

  type: "pipeline" | "powerline" | "telephone";
  subtype: utility_subtype | null;
  support_type: utility_supporttype | null;
  status: string | null;
  visibility: utility_visibility | null;
  
  ...TopographicMetadata;
  ...GeoParquet
}

enum utility_subtype {
  Ironsand: "ironsand",
  Sewage: "sewage",
  Steam: "steam",
  Water: "water",
}


enum utility_supporttype {
  Pole: "pole",
  Pylon: "pylon",
}

enum utility_visibility {
  Underground: "underground",
}