import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/utility_line")
@extension("additionalProperties", false)
model utility_line {
  ...TopographicFeature;

  type: "pipeline" | "powerline" | "telephone";
  utility_use: Topo.UtilityUseEnum | null;
  support_type: Topo.SupportTypeEnum | null;
  status: Topo.StatusEnum | null;
  name: string | null;
  visibility: Topo.VisibilityEnum | null;
  
  ...TopographicMetadata;
  ...GeoParquet
}

