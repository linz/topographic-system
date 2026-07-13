import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/tunnel_line")
@extension("additionalProperties", false)
model tunnel_line {
  ...TopographicFeature;

  type: "tunnel";
  tunnel_use: Topo.TunnelUseEnum | null;
  tunnel_use2: Topo.TunnelUse2Enum | null;
  subtype: Topo.TunnelSubTypeEnum | null;
  status: Topo.StatusEnum | null;
  name: string | null;
  
  ...TopographicMetadata;
  ...GeoParquet
}

