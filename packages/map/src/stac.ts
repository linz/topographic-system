import { type BoundingBox, Bounds } from '@basemaps/geo';
import { createStacCollection } from '@topographic-system/shared/src/stac.factory.ts';
import type { StacCollection, StacItem, StacLink } from 'stac-ts';

import type { ExportFormat } from './cli/action.produce.cover.ts';
import type { SheetMetadata } from './python.runner.ts';

export interface ExportOptions {
  /** layout name used for export, must be exist in the qgis project */
  layout: string;
  /** map sheet layer name used for export */
  mapSheetLayer: string;
  /** Creation Format  */
  format: ExportFormat;
  /** Creation dpi */
  dpi: number;
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

export function createMapSheetStacCollection(metadata: SheetMetadata[], links: StacLink[]): StacCollection {
  const allBbox: BoundingBox[] = [];
  for (const item of metadata) {
    if (item.bbox) allBbox.push(Bounds.fromBbox(item.bbox));
    links.push({
      rel: 'item',
      href: `./${item.sheetCode}.json`,
    });
  }
  const description = 'LINZ Topographic System Generated Maps.';
  const bbox = Bounds.union(allBbox).toBbox();

  return createStacCollection(description, bbox, links);
}
