import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/geographic_name")
@extension("additionalProperties", false)
model geographic_name {  
  ...TopographicFeature;

  type: "geographic_name";
  name: string | null;
  desc_code: string | null;
  size: float64 | null;
  
  theme: "annotation";
  ...TopographicMetadata;
  ...GeoParquet
}


