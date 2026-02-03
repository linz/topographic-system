import { fsa } from '@chunkd/fs';
import { createHash } from 'crypto';
import { basename } from 'path';
import type { StacAsset, StacCatalog, StacCollection, StacItem, StacLink, StacProvider } from 'stac-ts';
import type { GeoJSONGeometry } from 'stac-ts/src/types/geojson.d.ts';

import { CliDate, CliId, CliInfo } from './cli.info.ts';
import { S3BucketName } from './env.ts';
import { logger } from './log.ts';

export const RootCatalogFile = new URL(`s3://${S3BucketName}/catalog.json`);
const MediaTypes = {
  parquet: 'application/vnd.apache.parquet',
  geojson: 'application/geo+json',
  json: 'application/json',
  gpkg: 'application/geopackage+sqlite3',
  '': 'application/octet-stream',
};
const Roles = {
  parquet: 'data',
  geojson: 'data',
  json: 'metadata',
  gpkg: 'data',
  '': 'data',
};

const Providers: StacProvider[] = [
  { name: 'Land Information New Zealand', url: 'https://www.linz.govt.nz/', roles: ['processor', 'host'] },
];

// FIXME: This function is very specific to the Map Production use case ("project" and "source" link relations are not standard STAC).
//  May need to be generalized or move to Map package.
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

async function createStacCatalogFromFilename(stacFile: URL): Promise<StacCatalog> {
  if (await fsa.exists(stacFile)) {
    return await fsa.readJson<StacCatalog>(stacFile);
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

export async function createStacAssetFromFileName(assetFile: URL): Promise<StacAsset> {
  const stacAsset = createBasicStacAsset();
  const extension = assetFile.href.split('.').pop() || '';
  const dataset = basename(assetFile.href, `.${extension}`);
  const datatype = (extension in MediaTypes ? extension : '') as keyof typeof MediaTypes;

  if (await fsa.exists(assetFile)) {
    const assetStats = createFileStats(await fsa.read(assetFile));
    stacAsset.href = assetFile.href;
    stacAsset.title = dataset;
    stacAsset.description = `${dataset} data in ${extension} format`;
    stacAsset.type = MediaTypes[datatype];
    stacAsset.roles = [Roles[datatype]];
    stacAsset['file:size'] = assetStats['file:size'];
    stacAsset['file:checksum'] = assetStats['file:checksum'];
  }
  return stacAsset;
}

/**
 * Given a data asset path, create or update the corresponding STAC Item with the asset information.
 * Note:
 * One STAC Item may contain multiple assets.
 * Running this function multiple times with different assets for the same dataset will update the same STAC Item.
 * Running this in parallel for different assets of the same dataset may lead to race conditions,
 * as STAC Collection and Catalogs up to the RootCatalog are updated.
 *
 * @param assetFile - The URL of the data asset to be added to the STAC Item.
 * @param stacItemFile - Optional URL of the STAC Item file. If not provided, it will be derived from the data asset path.
 *
 * @returns The updated or newly created STAC Item, which includes the new asset and has been saved to s3.
 * */
export async function upsertAssetToItem(assetFile: URL, stacItemFile?: URL): Promise<URL> {
  const extension = assetFile.href.split('.').pop() ?? '';
  const dataset = basename(assetFile.href, `.${extension}`);
  const stacAsset = await createStacAssetFromFileName(assetFile);
  if (!stacItemFile) {
    stacItemFile = new URL(`./${dataset}.json`, assetFile);
  }
  const stacItem = await createStacItemFromFileName(stacItemFile);
  if (compareStacAssets(stacItem.assets[extension], stacAsset)) {
    logger.info({ dataset, asset: assetFile.href, stacItem: stacItemFile.href }, 'STAC:AssetInItemAlreadyUpToDate');
    return stacItemFile;
  }
  stacItem.assets[extension] = stacAsset;
  stacItem.properties.datetime = CliDate;
  await fsa.write(stacItemFile, JSON.stringify(stacItem, null, 2));
  logger.info({ dataset, asset: assetFile.href, stacItem: stacItemFile.href }, 'STAC:AssetInItemAddedOrUpdated');
  await upsertItemToCollection(stacItemFile);
  return stacItemFile;
}

/**
 * Given a STAC Item file, upsert it into the specified STAC Collection file.
 * If the STAC Collection file is not provided, it is assumed to be a STAC Collection located at './collection.json' relative to the STAC Item file.
 * This function updates the parent STAC Collection to include an "item" link to the STAC Item,
 * and adds to the temporal and spatial extents.
 * In order to keep all STAC files consistent, this function also triggers
 * an update of the parent Catalogs up to the RootCatalog
 * as well as an update of the ITEM's collection ID if necessary.
 *
 * @param stacItemFile - The URL of the STAC Item file to be upserted into the collection.
 * @param stacCollectionFile - Optional URL of the STAC Collection file. If not provided, it will be derived from the STAC Item file.
 *
 * @returns The URL of the updated or newly created STAC Collection file.
 */
export async function upsertItemToCollection(stacItemFile: URL, stacCollectionFile?: URL): Promise<URL> {
  if (!stacCollectionFile) {
    stacCollectionFile = new URL('./collection.json', stacItemFile);
  }

  let stacCollection = await createStacCollectionFromFileName(stacCollectionFile);
  let stacItem = await createStacItemFromFileName(stacItemFile);
  stacItem = addParentDataToChild(stacItem, stacCollection) as StacItem;
  stacCollection = addChildDataToParent(stacCollection, stacItem) as StacCollection;
  await fsa.write(stacItemFile, JSON.stringify(stacItem, null, 2));
  await fsa.write(stacCollectionFile, JSON.stringify(stacCollection, null, 2));
  logger.info({ stacCollectionFile: stacCollectionFile.href }, 'ToParquet:STACItemToCollectionUpserted');
  await upsertChildToCatalog(stacCollectionFile);

  return stacCollectionFile;
}

/** Given a STAC child file (Collection or Catalog), upsert it into its parent Catalog.
 * If the parent Catalog does not exist, it will be created.
 * This function will recursively ensure that all parent Catalogs up to the RootCatalog are updated.
 *
 * @param stacChildFile - The URL of the STAC child (Collection or Sub-Catalog) file to be upserted into the parent Catalog.
 * @param stacCatalogFile - Optional URL of the STAC parent (Catalog) file. If not provided, it will be derived from the STAC child file.
 *
 * @returns The URL of the updated or newly created STAC Catalog file.
 */
async function upsertChildToCatalog(stacChildFile: URL, stacCatalogFile?: URL): Promise<URL> {
  if (stacChildFile.href === RootCatalogFile.href) {
    logger.info({ stacChildFile: stacChildFile.href }, `STAC:ReachedRootCatalog`);
    return stacChildFile;
  }
  const childIsCollection = basename(stacChildFile.href) === 'collection.json';
  if (!stacCatalogFile) {
    stacCatalogFile = new URL('../catalog.json', stacChildFile);
  }
  let stacChild = childIsCollection
    ? await createStacCollectionFromFileName(stacChildFile)
    : await createStacCatalogFromFilename(stacChildFile);
  let stacCatalog = await createStacCatalogFromFilename(stacCatalogFile);
  stacChild = addParentDataToChild(stacChild, stacCatalog) as StacCollection | StacCatalog;
  stacCatalog = addChildDataToParent(stacCatalog, stacChild) as StacCatalog;

  await fsa.write(stacCatalogFile, JSON.stringify(stacCatalog, null, 2));
  logger.info(
    { stacChildFile: stacChildFile.href, stacCatalogFile: stacCatalogFile.href },
    `STAC:ChildToCatalogUpserted`,
  );
  await upsertChildToCatalog(stacCatalogFile);
  return stacCatalogFile;
}

/** Generate the STAC file:size and file:checksum fields from a buffer */
export function createFileStats(data: string | Buffer): { 'file:size': number; 'file:checksum': string } {
  return {
    'file:size': Buffer.isBuffer(data) ? data.byteLength : data.length,
    // Multihash header for sha256 is 0x12 0x20
    'file:checksum': '1220' + createHash('sha256').update(data).digest('hex'),
  };
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

function compareStacAssets(a: StacAsset | StacLink | undefined, b: StacAsset | StacLink | undefined): boolean {
  if (a && b)
    return (
      a.href === b.href &&
      a.type === b.type &&
      a['file:checksum'] === b['file:checksum'] &&
      a['file:size'] === b['file:size']
    );
  return false;
}

function urlToTitle(fileName: URL): string {
  return fileName.pathname.replace(/[/_-]/g, ' ').replace(/\.json$/, '');
}

/**
 * Ensure that the STAC child has a link to the specified STAC parent.
 * If the link is missing or incorrect, it will be added or updated.
 * If the child is a STAC Item, its collection ID will also be updated to match the parent's ID.
 *
 * @param stacChild - The STAC child (Item, Collection, or Catalog).
 * @param stacParent - The STAC parent (Collection or Catalog).
 *
 * @returns The updated STAC child with the correct parent link and collection ID (if applicable).
 */
function addParentDataToChild(
  stacChild: StacItem | StacCollection | StacCatalog,
  stacParent: StacCollection | StacCatalog,
): StacItem | StacCollection | StacCatalog {
  const stacParentFile = getSelfLink(stacParent);
  const stacChildFile = getSelfLink(stacChild);
  const childIsItem = stacChild.type === 'Feature';
  const parentIsCollection = stacParent.type === 'Collection';
  if (childIsItem && parentIsCollection) {
    // Set Collection link and update Collection ID for Item
    const collectionLink = stacChild.links.find((link) => link.rel === 'collection');
    if (collectionLink === undefined) {
      stacChild.links.push({ rel: 'collection', href: stacParentFile, type: 'application/json' });
      stacChild.collection = stacParent.id;
      stacChild.properties.datetime = CliDate;
      logger.info(
        { stacChildFile, collection: stacChild.collection, stacParentFile },
        'STAC:ItemCollectionLinkAndIdAdded',
      );
    } else if (collectionLink.href === getSelfLink(stacParent) && stacChild.collection !== stacParent.id) {
      // Links to this collection, but collection ID is wrong
      stacChild.collection = stacParent.id;
      stacChild.properties.datetime = CliDate;
      logger.info({ stacChildFile, collection: stacChild.collection, stacParentFile }, 'STAC:ItemCollectionIdUpdated');
    } else {
      logger.info(
        {
          stacChildFile,
          stacParentFile,
          collectionLink: collectionLink.href,
          childCollectionId: stacChild.collection,
          parentId: stacParent.id,
        },
        'STAC:ItemCollectionLinkAndIdUpToDate',
      );
    }
  }
  const parentLink = stacChild.links.find((link) => link.rel === 'parent');
  if (parentLink === undefined) {
    stacChild.links.push({ rel: 'parent', href: stacParentFile, type: 'application/json' });
    if (childIsItem) {
      stacChild.properties.datetime = CliDate;
    } else {
      stacChild['updated'] = CliDate;
    }
    logger.info({ stacChildFile, stacParentFile }, 'STAC:ParentLinkAdded');
    return stacChild;
  }
  if (parentLink.href !== stacParentFile) {
    logger.info({ stacChildFile, stacParentFile }, 'STAC:ChildHasDifferentParentLink');
    return stacChild;
  }
  logger.info({ stacChildFile, stacParentFile }, 'STAC:ParentLinkAlreadyUpToDate');
  return stacChild;
}

/**
 * Ensure that the STAC parent has a link to the specified STAC child.
 * If the link is missing or incorrect, it will be added or updated.
 * If the child is a STAC Item and the parent is a STAC Collection,
 * the parent's spatial and temporal extents will also be updated to include the child's extents.
 *
 * @param stacParent
 * @param stacChild
 *
 * @returns The updated STAC parent with the correct child link and updated extents (if applicable).
 */
function addChildDataToParent(
  stacParent: StacCollection | StacCatalog,
  stacChild: StacItem | StacCollection | StacCatalog,
): StacCollection | StacCatalog {
  const stacParentFile = getSelfLink(stacParent);
  const stacChildFile = getSelfLink(stacChild);
  const childIsItem = stacChild.type === 'Feature';
  const parentIsCollection = stacParent.type === 'Collection';
  const expectedRel = childIsItem ? 'item' : 'child';
  const expectedType = childIsItem ? 'application/geo+json' : 'application/json';
  const newLinkStats = createFileStats(JSON.stringify(stacChild, null, 2));
  const newLinkToChild = <StacLink>{
    rel: expectedRel,
    href: stacChildFile,
    type: expectedType,
    ...newLinkStats,
    title: stacChild.title,
  };

  const oldLinkToChild = stacParent.links.find(
    (link) => link.href === stacChildFile && link.rel === expectedRel && link.type === expectedType,
  );
  if (oldLinkToChild === undefined) {
    stacParent.links.push(newLinkToChild);
    stacParent['updated'] = CliDate;
    if (childIsItem && parentIsCollection) {
      logger.info({ stacChildFile, stacParentFile }, 'STAC:ChildLinkAndExtentsAdded');
      return addExtentFromItemToCollection(stacParent, stacChild);
    }
    logger.info({ stacChildFile, stacParentFile }, 'STAC:ChildLinkAdded');
    return stacParent;
  }
  if (compareStacAssets(newLinkToChild, oldLinkToChild)) {
    logger.info({ stacChildFile, stacParentFile }, 'STAC:ChildLinkAlreadyUpToDate');
    return stacParent;
  }
  stacParent.links = stacParent.links.filter(
    (link) => !(link.href === stacChildFile && link.rel === expectedRel && link.type === expectedType),
  );
  stacParent.links.push(newLinkToChild);
  stacParent['updated'] = CliDate;
  // FIXME: update extents in Collection when updating existing Items?
  //  To do this reliably, we need to read all child items again.
  //  For now, we skip updating extents on link updates.
  logger.info({ stacChildFile, stacParentFile }, 'STAC:ChildLinkUpdated');
  return stacParent;
}

function getSelfLink(stac: StacItem | StacCollection | StacCatalog): string {
  const selfLink = stac.links.find((link) => link.rel === 'self');
  if (selfLink === undefined) {
    logger.error({ stac }, 'STAC:SelfLinkUndefined');
    throw new Error('STAC self link is undefined');
  }
  return selfLink.href;
}

function addExtentFromItemToCollection(stacCollection: StacCollection, stacItem: StacItem): StacCollection {
  if (stacItem.bbox) {
    stacCollection.extent.spatial.bbox.push(stacItem.bbox);
  }
  if (stacItem.properties.start_datetime) {
    stacCollection.extent.temporal.interval.push([
      stacItem.properties.start_datetime,
      stacItem.properties.end_datetime ?? null,
    ]);
  }
  return stacCollection;
}
