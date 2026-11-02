import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/trig_point")
@extension("additionalProperties", false)
model trig_point {
  ...TopographicFeature;
  type: "beaconed" | "unbeaconed";
  name: string | null;
  code: string | null;
  elevation: int32 | null;
  theme: Theme.Relief;
  ...TopographicMetadata;
  ...GeoParquet;
}
