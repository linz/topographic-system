import { type BoundingBox, Bounds } from '@basemaps/geo';
import { fsa } from '@chunkd/fs';
import { logger } from '@topographic-system/shared/src/log.ts';
import { createFileStats, createStacCollection, createStacItem } from '@topographic-system/shared/src/stac.ts';
import type { StacAsset, StacCollection, StacItem, StacLink } from 'stac-ts';

import { type ExportFormat, getContentType } from './cli/action.produce.ts';
import { type SheetMetadata } from './python.runner.ts';

export interface CreationOptions {
  /** Map Sheet Code*/
  mapsheet: string;
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
    'linz_topographic_system:options'?: CreationOptions;
  };
};

function getExtentFormat(format: ExportFormat): string {
  if (format === 'pdf') return 'pdf';
  else if (format === 'tiff' || format === 'geotiff') return 'tiff';
  else if (format === 'png') return 'png';
  else throw new Error(`Invalid format`);
}

export async function createMapSheetStacItem(
  metadata: SheetMetadata,
  format: ExportFormat,
  dpi: number,
  outputUrl: URL,
  links: StacLink[],
): Promise<MapSheetStacItem> {
  logger.info({ sheetCode: metadata.sheetCode }, 'Stac: CreateStacItem');
  // Check asset been uploaded
  const extent = getExtentFormat(format);
  const filename = `${metadata.sheetCode}.${extent}`;
  const assetPath = new URL(filename, outputUrl);
  const data = await fsa.read(assetPath);
  if (data == null) throw new Error(`Stac: Asset not found for sheet ${metadata.sheetCode} at ${assetPath.href}`);
  const assets = {
    extent: {
      href: `./${filename}`,
      type: getContentType(format),
      roles: ['data'],
      ...createFileStats(data),
    } as StacAsset,
  };

  const item = createStacItem(metadata.sheetCode, links, assets, metadata.geometry, metadata.bbox) as MapSheetStacItem;

  item.properties['proj:epsg'] = metadata.epsg;
  item.properties['linz_topographic_system:options'] = {
    mapsheet: metadata.sheetCode,
    format,
    dpi,
  };

  return item;
}

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
