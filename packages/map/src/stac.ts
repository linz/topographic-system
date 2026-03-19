import { type BoundingBox, Bounds } from '@basemaps/geo';
import { fsa } from '@chunkd/fs';
import { createStacCatalog, createStacCollection } from '@linzjs/topographic-system-shared';
import type { StacCatalog, StacCollection, StacItem, StacLink } from 'stac-ts';

import { type ExportFormat, sheetCodeToPath } from './cli/action.produce.cover.ts';
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

export function createMapSheetStacCollection(
  rootCatalog: URL,
  metadata: SheetMetadata[],
  links: StacLink[],
): StacCollection {
  const allBbox: BoundingBox[] = [];
  for (const item of metadata) {
    if (item.bbox) allBbox.push(Bounds.fromBbox(item.bbox));
    links.push({
      rel: 'item',
      href: `./${sheetCodeToPath(item.sheetCode)}.json`,
    });
  }
  const description = 'LINZ Topographic System Generated Maps.';
  const bbox = Bounds.union(allBbox).toBbox();

  return createStacCollection(rootCatalog, description, bbox, links);
}

export async function createCatalog(
  path: URL,
  rootCatalog: URL,
  title: string,
  description: string,
  links: StacLink[],
): Promise<StacCatalog> {
  let catalog = createStacCatalog(rootCatalog, title, description, links);
  const existing = await fsa.exists(path);
  if (existing) {
    catalog = await fsa.readJson<StacCatalog>(path);
    for (const link of links) {
      if (catalog.links.find((l) => l.href === link.href)) continue;
      // Push new link if not exists
      catalog.links.push(link);
    }
  }
  return catalog;
}
