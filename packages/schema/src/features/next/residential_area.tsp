import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/residential_area")
@extension("additionalProperties", false)
model residential_area {
  ...TopographicFeature;

  type: "residential_area";
  name: string | null;

  theme: "landuse";
  ...TopographicMetadata;
  ...GeoParquet
}


