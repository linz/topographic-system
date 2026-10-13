import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("nztopo50_rock_line")
@extension("additionalProperties", false)
model nztopo50_rock_line {
  ...TopographicFeature;

  /** UUID of the source `marine` rock polygon this boundary line was derived from. */
  marine_id: string;

  type: "rock";
  name: string | null;
  subtype: composition_subtype | null;
  ...TopographicMetadata;
  ...GeoParquet;
}
