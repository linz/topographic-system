import "@typespec/json-schema";
import "./common.tsp";
import "./landuse.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/landuse_point")
@extension("additionalProperties", false)
model landuse_point {
  ...TopographicFeature;
  type: "mine";
  subtype: "opencast" | "underground" | null;
  status: landuse_status | null;
  name: string | null;
  substance_extracted: substance_extracted | null;

  ...GeoParquet
}





