import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/marine")
@extension("additionalProperties", false)
model marine {
  ...TopographicFeature;
  type: "mangrove" | "reef" | "rock" | "shoal";
  subtype: composition_subtype | null;
  name: string | null;
  theme: Theme.Landcover;
  ...TopographicMetadata;
  ...GeoParquet;
}
