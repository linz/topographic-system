import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/relief")
@extension("additionalProperties", false)
model relief {
  ...TopographicFeature;

  type: "waterfall" | "rapid";
  name: string | null;
  height: float64 | null;

  ...TopographicMetadata;
  ...GeoParquet
}

