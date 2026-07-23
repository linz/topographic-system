import "@typespec/json-schema";
import "./common.tsp";
import "./landuse.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/landuse_line")
@extension("additionalProperties", false)
model landuse_line {
  ...TopographicFeature;

  type: landuse_type;
  subtype: landuse_subtype | null;
  name: string | null;

  theme: "landuse";
  ...TopographicMetadata;
  ...GeoParquet
}


