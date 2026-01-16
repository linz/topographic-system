import { fsa } from '@chunkd/fs';
import { createHash } from 'crypto';
import { basename } from 'path';
import type { StacAsset, StacCatalog, StacCollection, StacItem, StacLink, StacProvider } from 'stac-ts';
import type { GeoJSONGeometry } from 'stac-ts/src/types/geojson.ts';

import { CliDate, CliId, CliInfo } from './cli.info.ts';
import { logger } from './log.ts';

const Environment = process.env['ENVIRONMENT'] || 'dev';
const EnvLabel = Environment === 'prod' ? '' : `-${Environment}`;
const S3BucketName = `linz-topography${EnvLabel}`;
export const RootCatalogFile = new URL(`s3://${S3BucketName}/catalog.json`);

const Providers: StacProvider[] = [
  { name: 'Land Information New Zealand', url: 'https://www.linz.govt.nz/', roles: ['processor', 'host'] },
];

// FIXME: This function is very specific to the Map Production use case. May need to be generalized or moved to Map package.
export async function createStacLink(source: URL, project: URL): Promise<StacLink[]> {
  const links: StacLink[] = [];
  logger.info({ source: source.href, project: project.href }, 'Stac:PrepareStacLinks');
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
  id: string,
  links: StacLink[],
  assets: Record<string, StacAsset>,
  geometry?: GeoJSONGeometry,
  bbox?: number[],
): StacItem {
  const item: StacItem = {
    id,
    type: 'Feature',
    collection: CliId,
    stac_version: '1.0.0',
    stac_extensions: [],
    geometry: geometry ?? null,
    bbox: bbox ?? [],
    links: [
      { href: `./${id}.json`, rel: 'self', type: 'application/geo+json' },
      { href: './collection.json', rel: 'collection', type: 'application/json' },
      { href: './collection.json', rel: 'parent', type: 'application/json' },
      { href: RootCatalogFile.href, rel: 'root', type: 'application/json' },
      ...links,
    ],
    properties: {
      datetime: CliDate,
      'linz_topographic_system:generated': {
        package: CliInfo.package,
        hash: CliInfo.hash,
        version: CliInfo.version,
        datetime: CliDate,
      },
    },
    assets: assets,
  };

  return item;
}

export function createStacCollection(description: string, bbox: number[], links: StacLink[]): StacCollection {
  return {
    stac_version: '1.0.0',
    stac_extensions: [],
    type: 'Collection',
    license: 'CC-BY-4.0',
    id: 'sc_' + CliId,
    description: description,
    extent: {
      spatial: {
        bbox: [bbox],
      },
      temporal: { interval: [[CliDate, null]] },
    },
    links: [{ rel: 'self', href: './collection.json', type: 'application/json' }, ...links],
    providers: Providers,
    summaries: {},
  };
}

export function createStacCatalog(title: string, description: string, links: StacLink[]): StacCatalog {
  return {
    stac_version: '1.0.0',
    stac_extensions: [],
    type: 'Catalog',
    title,
    description,
    id: 'sl_' + CliId,
    links: [{ rel: 'self', href: './catalog.json', type: 'application/json' }, ...links],
  };
}

/** Generate the STAC file:size and file:checksum fields from a buffer */
export function createFileStats(data: string | Buffer): { 'file:size': number; 'file:checksum': string } {
  return {
    'file:size': Buffer.isBuffer(data) ? data.byteLength : data.length,
    // Multihash header for sha256 is 0x12 0x20
    'file:checksum': '1220' + createHash('sha256').update(data).digest('hex'),
  };
}
