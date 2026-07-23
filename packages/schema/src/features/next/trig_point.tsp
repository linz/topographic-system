import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/trig_point")
@extension("additionalProperties", false)
model trig_point {
  ...TopographicFeature;

  type: "trig";
  trig_type: "beaconed" | null;
  name: string | null;
  code: string | null;
  elevation: int32 | null;
  
  theme: "relief";
  ...TopographicMetadata;
  ...GeoParquet
}


