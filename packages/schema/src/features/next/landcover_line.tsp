import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/landcover_line")
@extension("additionalProperties", false)
model landcover_line {
  ...TopographicFeature;

  type: "dredge_tailing";
  
  theme: "landuse";
  ...TopographicMetadata;
  ...GeoParquet
}


