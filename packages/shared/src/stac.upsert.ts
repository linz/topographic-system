import { fsa } from '@chunkd/fs';
import { basename } from 'path';
import type { StacCatalog } from 'stac-ts';
import type { StacCollection, StacItem } from 'stac-ts';

import { CliDate } from './cli.info.ts';
import { serializeBigInt } from './json.utils.ts';
import { logger } from './log.ts';
import type { ColumnStats } from './parquet.metadata.ts';
import { extractSpatialExtent, extractTemporalExtent } from './parquet.metadata.ts';
import { RootCatalogFile } from './stac.constants.ts';
import {
  createStacAssetFromFileName,
  createStacCatalogFromFilename,
  createStacCollectionFromFileName,
  createStacItemFromFileName,
} from './stac.factory.ts';
import { compareStacAssets } from './stac.links.ts';
import { addChildDataToParent, addParentDataToChild } from './stac.links.ts';

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
 * Given a data asset path, create or update the corresponding STAC Catalog with the asset information.
 * Note:
 * One STAC Collection per geoparquet file, with additional related assets.
 *
 * @param assetFile - The URL of the data asset to be added to the STAC Item.
 * @param stacCollectionFile - Optional URL of the STAC Item file. If not provided, it will be derived from the data asset path.
 *
 * @returns The updated or newly created STAC Item, which includes the new asset and has been saved to s3.
 * */
export async function upsertAssetToCollection(assetFile: URL, stacCollectionFile?: URL): Promise<URL> {
  const extension = assetFile.href.split('.').pop() ?? '';
  const dataset = basename(assetFile.href, `.${extension}`);
  const stacAsset = await createStacAssetFromFileName(assetFile);
  if (!stacCollectionFile) {
    stacCollectionFile = new URL(`./collection.json`, assetFile);
  }
  const stacCollection = await createStacCollectionFromFileName(stacCollectionFile);
  stacCollection['assets'] = stacCollection['assets'] || {};
  if (compareStacAssets(stacCollection.assets['data'], stacAsset)) {
    logger.info(
      { dataset, asset: assetFile.href, stacCollection: stacCollectionFile.href },
      'STAC:AssetInItemAlreadyUpToDate',
    );
    return stacCollectionFile;
  }
  stacCollection.assets[extension] = stacAsset;
  if (extension === 'parquet') {
    const bbox = extractSpatialExtent(stacAsset['table:columns'] as ColumnStats[]);
    const dates = extractTemporalExtent(stacAsset['table:columns'] as ColumnStats[]);
    stacCollection['extent'] = { spatial: { bbox: [bbox] }, temporal: { interval: [dates] } };
  }

  await fsa.write(stacCollectionFile, JSON.stringify(stacCollection, serializeBigInt, 2));
  logger.info({ dataset, asset: assetFile.href, stacItem: stacCollectionFile.href }, 'STAC:AssetInItemAddedOrUpdated');
  await upsertItemToCollection(stacCollectionFile);
  return stacCollectionFile;
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
  await fsa.write(stacCollectionFile, JSON.stringify(stacCollection, serializeBigInt, 2));
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

export async function getDataFromCatalog(stacUrl: URL, layerName: string, tag: string = 'latest'): Promise<URL> {
  if (stacUrl.href.endsWith('catalog.json')) {
    const catalog = await fsa.readJson<StacCatalog>(stacUrl);
    const childLink = catalog.links.find((link) => link.rel === 'child' && link.href.includes(`/${layerName}/`));
    if (!childLink) throw new Error(`Layer ${layerName} not found in catalog ${stacUrl.href}`);

    // Recursively search in child catalog
    if (childLink.href.endsWith('catalog.json')) return getDataFromCatalog(new URL(childLink.href), layerName, tag);

    // Found collection link
    if (childLink.href.endsWith('collection.json')) {
      if (childLink.href.includes(`/${tag}/`)) return new URL(childLink.href);
    }
  }

  throw new Error(`Layer ${layerName} with tag ${tag} not found in catalog ${stacUrl.href}`);
}
