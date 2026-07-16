import "@typespec/json-schema";
using TypeSpec.JsonSchema;
/** GeoParquet 1.1 covering bbox struct. */
@extension("additionalProperties", false)
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
  created_at: utcDateTime;
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

model TopographicMetadata {
  metadata: DataSource[] | null;
}
@extension("additionalProperties", false)
model DataSource {
/** Name of the column in this table where the linked data gets copied to */
  table_column: string;
/** Registered source for linked data */
  source: metadata_source;
/** Name of the key column in source to use for linking */
  source_key_name: string;
/** Value of the key column in source to use for linking. */
  source_key_value: uint32 | string;  
/** Name of the table in source that contains the linked data */
  source_table: string;
/** Name of the column in the source where the linked data gets copied from */
  source_column: string;
/** Timestamp when the source was last updated */
  source_updated_at: utcDateTime;
/** Timestamp when this linked data was last imported */
  imported_at: utcDateTime;
}

enum metadata_source  {
  NZGBGazetteer: "nzgb_gazetteer",
  LINZAIMS: "linz_aims",
}