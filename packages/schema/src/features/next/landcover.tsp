import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/landcover")
@extension("additionalProperties", false)
model landcover {
  ...TopographicFeature;


  type: "ice" | "moraine" | "moraine_wall" | "mud" | "sand" | "scree" | "shingle" | "swamp";
  name: string | null;
 
  ...GeoParquet
}

