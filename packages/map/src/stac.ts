import type { StacItem } from 'stac-ts';

import { type ExportFormat } from './cli/action.produce.cover.ts';

export interface ExportOptions {
  /** layout name used for export, must be exist in the qgis project */
  layout: string;
  /** map sheet layer name used for export */
  mapSheetLayer: string;
  /** Creation Format  */
  format: ExportFormat;
  /** Creation dpi */
  dpi: number;
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
