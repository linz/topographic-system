import "@typespec/json-schema";
import "@typespec/versioning";
import "./common.tsp";
import "../version.tsp";

using TypeSpec.JsonSchema;
using TypeSpec.Versioning;

namespace Topography;

/**
 * A facility where aircraft can take off and land;
 * usually consists of hard-surfaced landing strips, a control tower, hangars and accommodation for passengers and cargo
 */
@jsonSchema("airport")
@extension("additionalProperties", false)
@summary("NZ Topography Airport")
model airport {
  ...TopographicFeature;

  @removed(Versions.v0_3)
  type: "airport";

  name: string | null;
  theme: Theme.Transport;
  ...TopographicMetadata;
  ...GeoParquet;
}
