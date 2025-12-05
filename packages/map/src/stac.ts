import { type BoundingBox, Bounds } from '@basemaps/geo';
import { fsa } from '@chunkd/fs';
import { basename } from 'path';
import type { StacCatalog, StacCollection, StacItem, StacLink, StacProvider } from 'stac-ts';

import { CliDate, CliId, CliInfo } from './cli.info.ts';
import type { ExportFormat } from './cli/action.produce.ts';
import { logger } from './log.ts';
import type { SheetMetadata } from './python.runner.ts';

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

export type GeneratedStacItem = StacItem & {
  properties: {
    'linz_topographic_system:generated': GeneratedProperties;
    'linz_topographic_system:options'?: CreationOptions;
  };
};

const providers: StacProvider[] = [
  { name: 'Land Information New Zealand', url: 'https://www.linz.govt.nz/', roles: ['processor', 'host'] },
];

export async function createStacLink(source: URL, project: URL): Promise<StacLink[]> {
  const links: StacLink[] = [];
  logger.info({ source: source.href, project: project.href }, 'Stac:PreareStacLinks');
  // Create stac link for external layer

  links.push({
    rel: 'project',
    name: basename(project.pathname),
    href: project.href,
  });

  for await (const file of fsa.list(source)) {
    links.push({
      rel: 'source',
      name: basename(file.pathname),
      href: file.href,
    });
  }
  return links;
}

export function createStacItem(
  metadata: SheetMetadata,
  format: ExportFormat,
  dpi: number,
  links: StacLink[],
): GeneratedStacItem {
  logger.info({ sheetCode: metadata.sheetCode }, 'Stac: CreateStacItem');
  const item: GeneratedStacItem = {
    id: `${CliId}/${metadata.sheetCode}`,
    type: 'Feature',
    collection: CliId,
    stac_version: '1.0.0',
    stac_extensions: [],
    geometry: metadata.geometry,
    bbox: metadata.bbox,
    links: [
      { href: `./${metadata.sheetCode}.json`, rel: 'self' },
      { href: './collection.json', rel: 'collection', type: 'application/json' },
      { href: './collection.json', rel: 'parent', type: 'application/json' },
      ...links,
    ],
    properties: {
      datetime: CliDate,
      'proj:epsg': metadata.epsg,
      'linz_topograhpic_system:generated': {
        package: CliInfo.package,
        hash: CliInfo.hash,
        version: CliInfo.version,
        datetime: CliDate,
      },
      'linz_topograhpic_system:options': {
        mapsheet: metadata.sheetCode,
        format,
        dpi,
      },
    },
    assets: {},
  };

  return item;
}

export function createStacCollection(metadata: SheetMetadata[], links: StacLink[]): StacCollection {
  const allBbox: BoundingBox[] = [];
  const itemLinks: StacLink[] = [];
  for (const item of metadata) {
    if (item.bbox) allBbox.push(Bounds.fromBbox(item.bbox));
    itemLinks.push({
      rel: 'item',
      href: `./${item.sheetCode}.json`,
    });
  }

  return {
    stac_version: '1.0.0',
    stac_extensions: [],
    type: 'Collection',
    license: 'CC-BY-4.0',
    id: 'sc_' + CliId,
    description: 'LINZ Topographic System Generated Maps.',
    extent: {
      spatial: {
        bbox: [Bounds.union(allBbox).toBbox()],
      },
      temporal: { interval: [[CliDate, null]] },
    },
    links: [{ rel: 'self', href: './collection.json', type: 'application/json' }, ...itemLinks, ...links],
    providers,
    summaries: {},
  };
}

export function createStacCatalog(): StacCatalog {
  return {
    stac_version: '1.0.0',
    stac_extensions: [],
    type: 'Catalog',
    title: 'Topographic System Map Producer',
    description: 'Topographic System Map Producer to generate maps from Qgis project in pdf, tiff, geotiff formats',
    id: 'sl_' + CliId,
    links: [
      { rel: 'self', href: './catalog.json', type: 'application/json' },
      {
        rel: 'collection',
        href: `./${CliId}/collection.json`,
        type: 'application/json',
      },
    ],
  };
}
