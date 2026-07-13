import "@typespec/json-schema";

using TypeSpec.JsonSchema;

/** GeoParquet 1.1 covering bbox struct. */
model BBox {
  xmin: float32;
  ymin: float32;
  xmax: float32;
  ymax: float32;
}

model GeoParquet {
  /** GeoParquet 1.1 covering geometry struct. */
  @extension("not", #{ type: "null" })
  geometry: unknown;

  /** GeoParquet 1.1 covering bbox struct. */
  bbox?: BBox;
}

model TopographicFeature {
  /** UUIDv7 of the feature */
  id: string;
  /** ISO Datetime of when the feature was created  */
  created_at: utcDateTime;
  /** ISO Datetime of when the feature was last updated */
  updated_at: utcDateTime;

  /**
   * Reference topo50 feature ID.
   * 
   * Will be null if the feature is new and has not been published in a Topo50 edition.
   */
  t50_fid: uint32 | null;
}

model ProductFeature {
  /** UUIDv7 of the feature */
  id: string;

  /**
   * Reference topo50 feature ID.
   * 
   * Will be null if the feature is new and has not been published in a Topo50 edition.
   */
  t50_fid: uint32 | null;
}