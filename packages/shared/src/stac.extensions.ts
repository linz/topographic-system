export const StacExtensions = {
  file: 'https://stac-extensions.github.io/file/v2.1.0/schema.json',
  proj: 'https://stac-extensions.github.io/projection/v2.0.0/schema.json',
  table: 'https://stac-extensions.github.io/table/v1.3.0/schema.json',
} as const;

export type StacExtensionUrl = (typeof StacExtensions)[keyof typeof StacExtensions];

export interface StacProjectionV2_0_0 {
  /** Authority and specific code of the data source (e.g., EPSG:3857)  */
  'proj:epsg'?: number;
  /** WKT2 string representing the Coordinate Reference System (CRS) that the proj:geometry and proj:bbox fields represent */
  'proj:wkt2'?: string;
  /** PROJJSON object representing the Coordinate Reference System (CRS) that the proj:geometry and proj:bbox fields represent  */
  'proj:projjson'?: unknown;
  /** Defines the footprint of this Item. (geojson) */
  'proj:geometry'?: unknown;
  /** The affine transformation coefficients for the default grid  */
  'proj:transform'?: number[];
  /** Number of pixels in Y and X directions for the default grid */
  'proj:shape'?: number[];
  /** Coordinates representing the centroid of the Item (in lat/long)  */
  'proj:centroid'?: number[];
  /** Bounding box of the Item in the asset CRS in 2 or 3 dimensions.  */
  'proj:bbox'?: number[];
}

export interface StacTableV1_3_0Column {
  /** The column name. */
  name: string;
  /** Detailed multi-line description to explain the dimension. CommonMark 0.29 syntax MAY be used for rich text representation. */
  description?: string;
  /** Native data type of the column. If using a file format with a type system (like Parquet), we recommend you use those types. */
  type?: string;
}

export interface StacTableV1_3_0 {
  /** A list of Column Objects describing each column. */
  'table:columns'?: StacTableV1_3_0Column[];
  /** The primary geometry column name. */
  'table:primary_geometry'?: string;
  /** The primary date/time column name. */
  'table:primary_datetime'?: string;
  /** The number of rows in the dataset. */
  'table:row_count'?: number;
}

export interface StacFileV2_1_0 {
  /** he byte order of integer values in the file. One of big-endian or little-endian. */
  'file:byte_order'?: string;
  /** Provides a way to specify file checksums (e.g. BLAKE2, MD5, SHA1, SHA2, SHA3). The hashes are self-identifying hashes as described in the Multihash specification and must be encoded as hexadecimal (base 16) string with lowercase letters. */
  'file:checksum'?: string;
  /** The header size of the file, specified in bytes. */
  'file:header_size'?: number;
  /** The file size, specified in bytes. */
  'file:size'?: number;
  /** A relative local path for the asset/link. */
  'file:local_path'?: string;
}
