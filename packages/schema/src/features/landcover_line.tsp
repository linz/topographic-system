import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("landcover_line")
@extension("additionalProperties", false)
model landcover_line {
  ...TopographicFeature;
  type: "dredge_tailing";
  theme: Theme.Landcover;
  ...TopographicMetadata;
  ...GeoParquet;
}
