import type { StacItem } from 'stac-ts';

import type { ExportAsset } from './cli/export.options.ts';

export interface ExportOptions {
  /**
   * map sheet dataset name used for export
   *
   * This is the dataset name of the dataset as it appears in the stac item json
   *
   * @example "nztopo50_map_sheet.parquet"
   */
  mapSheetDataset: string;
  /**
   * carto text dataset name used for export
   *
   * This is the dataset name of the dataset as it appears in the stac item json.
   *
   * @example "nztopo50_carto_text.parquet"
   */
  cartoTextDataset: string;

  /** Assets to create */
  assets: ExportAsset[];

  /** Optional list of layer names to exclude from export */
  excludeLayers?: string[];
}

export interface GeneratedProperties {
  /** Package name that generated the file */
  package: string;

  /** Version number that generated the file */
  version?: string;

  /** Git commit hash that the file was generated with */
  hash?: string;

  /** ISO date of the time this file was generated */
  datetime: string;
}

export type MapSheetStacItem = StacItem & {
  properties: {
    'linz_topographic_system:generated': GeneratedProperties;
    'linz_topographic_system:options'?: ExportOptions;
  };
};
