import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/vegetation")
@extension("additionalProperties", false)
model vegetation {
  ...TopographicFeature;

  type: "exotic" | "native" | "scattered_scrub" | "scrub";
  subtype: Topo.SubTypeEnum | null;
  species: Topo.SpeciesEnum | null;
  
  ...TopographicMetadata;
  ...GeoParquet
}

