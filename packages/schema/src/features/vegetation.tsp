import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

namespace Linz.Topography;

@jsonSchema("vegetation")
@oneOf
union vegetation {
  other: vegetation_other,
  exotic: vegetation_exotic,
}

@extension("additionalProperties", false)
model vegetation_exotic {
  ...TopographicFeature;
  type: "exotic";
  subtype: vegetation_subtype_exotic = vegetation_subtype_exotic.Coniferous;
  theme: Theme.Landcover;
  ...TopographicMetadata;
  ...GeoParquet;
}

enum vegetation_subtype_exotic {
  Coniferous: "coniferous",
  NonConiferous: "non-coniferous",
}

@extension("additionalProperties", false)
model vegetation_other {
  ...TopographicFeature;
  type: "native" | "scattered_scrub" | "scrub";
  subtype: null;
  theme: Theme.Landcover;
  ...TopographicMetadata;
  ...GeoParquet;
}
