import { fsa } from '@chunkd/fs';
import { HashTransform } from '@chunkd/fs/build/src/hash.stream.js';
import { createHash } from 'crypto';
import { basename } from 'path';
import type { StacAsset, StacCatalog, StacCollection, StacItem, StacLink } from 'stac-ts';
import type { GeoJSONGeometry } from 'stac-ts/src/types/geojson.d.ts';
import { Writable } from 'stream';
import { pipeline } from 'stream/promises';

import { CliDate, CliId, CliInfo } from './cli.info.ts';
import { deserializeBigInt, serializeBigInt } from './json.utils.ts';
import { logger } from './log.ts';
import type { RowGroupColumnStats } from './parquet.metadata.ts';
import { readParquetFileMetadata } from './parquet.metadata.ts';
import { mapParquetMetadataToStacStats } from './parquet.metadata.ts';
import { MediaTypes, Providers, Roles, RootCatalogFile } from './stac.constants.ts';
import { getSelfLink } from './stac.links.ts';

interface FileStats {
  'file:size': number;
  'file:checksum': string;
}

function createBasicStacAsset(): StacAsset {
  return {
    href: '',
    type: MediaTypes[''],
    roles: [Roles['']],
  };
}

function createBasicStacItem(): StacItem {
  return {
    id: '',
    type: 'Feature',
    collection: CliId,
    stac_version: '1.0.0',
    stac_extensions: [],
    geometry: null,
    bbox: [],
    links: [],
    properties: {
      datetime: CliDate,
      // TODO: Consider using STAC Processing extension?
      'linz_topographic_system:generated': {
        package: CliInfo.package,
        hash: CliInfo.hash,
        version: CliInfo.version,
        datetime: CliDate,
      },
    },
    assets: {},
  };
}

function createBasicStacCollection(): StacCollection {
  return {
    type: 'Collection',
    stac_version: '1.0.0',
    id: 'sc_' + CliId,
    description: '',
    extent: {
      spatial: {
        bbox: [[]],
      },
      temporal: {
        interval: [['', '']],
      },
    },
    links: [],
    license: 'CC-BY-4.0',
    created: CliDate,
    updated: CliDate,
    providers: Providers,
    stac_extensions: [],
    summaries: {},
  };
}

function createBasicStacCatalog(): StacCatalog {
  return {
    type: 'Catalog',
    stac_version: '1.0.0',
    stac_extensions: [],
    id: 'sl_' + CliId,
    title: '',
    description: '',
    links: [],
    created: CliDate,
    updated: CliDate,
  };
}

// FIXME: This function is very specific to the Map Production use case ("project" and "source" link relations are not standard STAC).
//  May need to be generalized or move to Map package.
export function createStacLink(sources: URL[], project: URL): StacLink[] {
  const links: StacLink[] = [];
  logger.info({ source: sources.map((s) => s.href), project: project.href }, 'Stac:PrepareStacLinks');
  // Create stac link for external layer

  links.push({
    rel: 'project',
    name: basename(project.pathname),
    href: project.href,
  });

  for (const file of sources) {
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
  const stacItem = createBasicStacItem();
  stacItem.id = id;
  stacItem.links.push(
    { rel: 'self', href: `./${id}.json`, type: 'application/geo+json' },
    { rel: 'collection', href: './collection.json', type: 'application/json' },
    { rel: 'parent', href: './collection.json', type: 'application/json' },
    { rel: 'root', href: RootCatalogFile.href, type: 'application/json' },
    ...links,
  );
  stacItem.assets = assets;
  if (geometry !== undefined) {
    stacItem.geometry = geometry;
  }
  if (bbox !== undefined) {
    stacItem.bbox = bbox;
  }
  return stacItem;
}

export function createStacCollection(description: string, bbox: number[], links: StacLink[]): StacCollection {
  const stacCollection = createBasicStacCollection();
  stacCollection.extent.spatial.bbox = [bbox];
  stacCollection.description = description;
  stacCollection.links.push(
    { rel: 'self', href: './collection.json', type: 'application/json' },
    { rel: 'parent', href: './catalog.json', type: 'application/json' },
    { rel: 'root', href: RootCatalogFile.href, type: 'application/json' },
    ...links,
  );
  return stacCollection;
}

export function createStacCatalog(title: string, description: string, links: StacLink[]): StacCatalog {
  const stacCatalog = createBasicStacCatalog();
  stacCatalog.title = title;
  stacCatalog.description = description;
  stacCatalog.links.push(
    { rel: 'self', href: './catalog.json', type: 'application/json' },
    { rel: 'root', href: RootCatalogFile.href, type: 'application/json' },
    ...links,
  );
  return stacCatalog;
}

export async function createStacAssetFromFileName(assetFile: URL): Promise<StacAsset> {
  const stacAsset = createBasicStacAsset();
  const extension = assetFile.href.split('.').pop() || '';
  const dataset = basename(assetFile.href, `.${extension}`);
  const datatype = (extension in MediaTypes ? extension : '') as keyof typeof MediaTypes;
  let fileStats = {} as FileStats;
  let parquetStats = {} as RowGroupColumnStats;

  if (await fsa.exists(assetFile)) {
    fileStats = await createFileStats(assetFile);
    if (extension === 'parquet') {
      const parquetMetadata = await readParquetFileMetadata(assetFile);
      parquetStats = mapParquetMetadataToStacStats(parquetMetadata);
    }
    stacAsset.href = assetFile.href;
    stacAsset.title = dataset;
    stacAsset.description = `${dataset} data in ${extension} format`;
    stacAsset.type = MediaTypes[datatype];
    stacAsset.roles = [Roles[datatype]];
  }

  return { ...stacAsset, ...fileStats, ...parquetStats };
}

export async function createStacItemFromFileName(stacFile: URL): Promise<StacItem> {
  if (await fsa.exists(stacFile)) {
    return await fsa.readJson<StacItem>(stacFile);
  }
  const stacItem = createBasicStacItem();
  stacItem.id = await readOrCreateStacIdFromFileName(stacFile);
  stacItem.links.push(
    { rel: 'root', href: RootCatalogFile.href, type: 'application/json' },
    { rel: 'self', href: stacFile.href, type: 'application/geo+json' },
  );
  return stacItem;
}

export async function createStacCollectionFromFileName(stacFile: URL): Promise<StacCollection> {
  if (await fsa.exists(stacFile)) {
    return await fsa.readJson<StacCollection>(stacFile);
  }
  const stacCollection = createBasicStacCollection();
  stacCollection.id = await readOrCreateStacIdFromFileName(stacFile);
  stacCollection.description = `Collection of${urlToTitle(stacFile)}`;
  stacCollection.links.push(
    { rel: 'root', href: RootCatalogFile.href, type: 'application/json' },
    { rel: 'self', href: stacFile.href, type: 'application/json' },
  );
  return stacCollection;
}

export async function createStacCatalogFromFilename(stacFile: URL): Promise<StacCatalog> {
  if (await fsa.exists(stacFile)) {
    const stacContent = await fsa.read(stacFile);
    return JSON.parse(stacContent.toString(), deserializeBigInt) as StacCatalog;
  }
  const stacCatalog = createBasicStacCatalog();
  stacCatalog.id = await readOrCreateStacIdFromFileName(stacFile);
  stacCatalog.description = `Catalog of${urlToTitle(stacFile)}`;
  stacCatalog.links.push(
    { rel: 'root', href: RootCatalogFile.href, type: 'application/json' },
    { rel: 'self', href: stacFile.href, type: 'application/json' },
  );

  if (getSelfLink(stacCatalog) === RootCatalogFile.href) {
    stacCatalog.title = 'LINZ Topographic Data Catalog';
    stacCatalog.description = 'Root Catalog of LINZ Topographic Data';
  }

  return stacCatalog;
}

/**
 * Serialize a STAC object to JSON string with consistent formatting.
 * Uses serializeBigInt for items and collections, null for catalogs.
 */
export function stacToJson(stac: StacItem | StacCollection | StacCatalog): string {
  // Items and Collections may contain BigInt fields from parquet metadata
  if (stac.type === 'Feature' || stac.type === 'Collection') {
    return JSON.stringify(stac, serializeBigInt, 2);
  }
  // Catalogs don't have BigInt fields
  return JSON.stringify(stac, null, 2);
}

/** Generate the STAC file:size and file:checksum fields by streaming a file */
export async function createFileStats(assetFile: URL): Promise<FileStats> {
  const hashStream = new HashTransform('sha256');
  await pipeline(
    fsa.readStream(assetFile),
    hashStream,
    new Writable({
      write(_chunk, _encoding, callback): void {
        callback();
      },
    }),
  );
  logger.info(
    { assetFile: assetFile.href, size: hashStream.size, hash: hashStream.multihash },
    'STAC:HashStreamCompleted',
  );
  return {
    'file:size': hashStream.size,
    'file:checksum': hashStream.multihash,
  };
}

/** Generate the STAC file:size and file:checksum fields from a STAC object by serializing to JSON */
export function createFileStatsFromStac(stac: StacItem | StacCollection | StacCatalog): FileStats {
  const jsonString = stacToJson(stac);
  const buffer = Buffer.from(jsonString);
  const hash = createHash('sha256').update(buffer).digest('hex');

  return {
    'file:size': buffer.byteLength,
    // Multihash header for sha256 is 0x12 0x20
    'file:checksum': '1220' + hash,
  };
}

function urlToTitle(fileName: URL): string {
  return fileName.pathname.replace(/[/_-]/g, ' ').replace(/\.json$/, '');
}

async function readOrCreateStacIdFromFileName(stacFile: URL): Promise<string> {
  if (await fsa.exists(stacFile)) {
    const stac = await fsa.readJson<StacCollection | StacCatalog>(stacFile);
    if (stac.id) {
      return stac.id;
    }
  }
  const timestamp = CliDate.replace(/[-:.Z]/g, '').replace('T', '-');
  const pathPart = stacFile.href
    .slice(stacFile.protocol.length + 2)
    .replaceAll('/', '-')
    .slice(0, -5);
  return `${pathPart}-${timestamp}`;
}
