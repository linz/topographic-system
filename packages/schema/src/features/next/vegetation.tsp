import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/vegetation")
@extension("additionalProperties", false)
model vegetation {
  ...TopographicFeature;

  type: "exotic" | "native" | "scattered_scrub" | "scrub";
  subtype: vegetation_subtype | null;
  species: string | null;
  
  theme: "landcover";
  ...TopographicMetadata;
  ...GeoParquet
}

enum vegetation_subtype {
  Coniferous: "coniferous",
  NonConiferous: "non-coniferous",
}

