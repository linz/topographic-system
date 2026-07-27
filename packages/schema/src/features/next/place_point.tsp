import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/place_point")
@extension("additionalProperties", false)
model place_point {
  ...TopographicFeature;
  type: place_type;
  subtype: composition_subtype | null;
  name: string | null;
  theme: Theme.Landuse;
  ...TopographicMetadata;
  ...GeoParquet;
}

enum place_type {
  Cave: "cave",
  Grave: "grave",
  HistoricSite: "historic_site",
  Monument: "monument",
  Pa: "pa",
}


