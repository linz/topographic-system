import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/place_point")
@extension("additionalProperties", false)
model place_point {
  ...TopographicFeature;
  type: place_type;
  subtype: place_subtype | null;
  name: string | null;
  elevation: int32 | null;
  composition: place_composition | null;
  description: string | null;

  ...GeoParquet
}

enum place_type {
  Cave: "cave",
  Cemetery: "cemetery",
  Grave: "grave",
  HistoricSite: "historic_site",
  Mine: "mine",
  Monument: "monument",
  PA: "pa",
}

enum place_subtype {
  Opencast: "opencast",
  Underground: "underground",
}

enum place_composition {
  Limestone: "limestone",
  Pumice: "pumice",
}