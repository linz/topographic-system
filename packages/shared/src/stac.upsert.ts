import { fsa } from '@chunkd/fs';
import { basename } from 'path';
import type { StacCatalog, StacCollection, StacItem } from 'stac-ts';

import { CliDate } from './cli.info.ts';
import { logger } from './log.ts';
import type { ColumnStats } from './parquet.metadata.ts';
import { extractSpatialExtent, extractTemporalExtent } from './parquet.metadata.ts';
import { RootCatalogFile } from './stac.constants.ts';
import {
  createStacAssetFromFileName,
  createStacCatalogFromFilename,
  createStacCollectionFromFileName,
  createStacItemFromFileName,
  stacToJson,
} from './stac.factory.ts';
import { addChildDataToParent, addParentDataToChild, compareStacAssets } from './stac.links.ts';

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
  if (stacItemFile == null) {
    stacItemFile = new URL(`./${dataset}.json`, assetFile);
  }
  const stacItem = await createStacItemFromFileName(stacItemFile);
  if (compareStacAssets(stacItem.assets[extension], stacAsset)) {
    logger.info({ dataset, asset: assetFile.href, stacItem: stacItemFile.href }, 'STAC:AssetInItemAlreadyUpToDate');
    return stacItemFile;
  }
  stacItem.assets[extension] = stacAsset;
  stacItem.properties.datetime = CliDate;
  await fsa.write(stacItemFile, stacToJson(stacItem));
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
export async function upsertAssetToCollection(
  assetFile: URL,
  stacCollectionFile: URL = new URL(`./collection.json`, assetFile),
): Promise<URL> {
  const extension = assetFile.href.split('.').pop() ?? '';
  const dataset = basename(assetFile.href, `.${extension}`);
  const stacAsset = await createStacAssetFromFileName(assetFile);
  const stacCollection = await createStacCollectionFromFileName(stacCollectionFile);
  stacCollection['assets'] = stacCollection['assets'] ?? {};
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

  await fsa.write(stacCollectionFile, stacToJson(stacCollection));
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
export async function upsertItemToCollection(
  stacItemFile: URL,
  stacCollectionFile: URL = new URL('./collection.json', stacItemFile),
): Promise<URL> {
  let [stacCollection, stacItem] = await Promise.all([
    createStacCollectionFromFileName(stacCollectionFile),
    createStacItemFromFileName(stacItemFile),
  ]);

  stacItem = addParentDataToChild(stacItem, stacCollection) as StacItem;
  stacCollection = addChildDataToParent(stacCollection, stacItem) as StacCollection;
  await Promise.all([
    fsa.write(stacItemFile, stacToJson(stacItem)),
    fsa.write(stacCollectionFile, stacToJson(stacCollection)),
  ]);

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
async function upsertChildToCatalog(
  stacChildFile: URL,
  stacCatalogFile: URL = new URL('../catalog.json', stacChildFile),
): Promise<URL> {
  if (stacChildFile.href === RootCatalogFile.href) {
    logger.info({ stacChildFile: stacChildFile.href }, `STAC:ReachedRootCatalog`);
    return stacChildFile;
  }
  const childIsCollection = basename(stacChildFile.href) === 'collection.json';
  let stacChild = childIsCollection
    ? await createStacCollectionFromFileName(stacChildFile)
    : await createStacCatalogFromFilename(stacChildFile);
  let stacCatalog = await createStacCatalogFromFilename(stacCatalogFile);
  stacChild = addParentDataToChild(stacChild, stacCatalog) as StacCollection | StacCatalog;
  stacCatalog = addChildDataToParent(stacCatalog, stacChild) as StacCatalog;

  await fsa.write(stacCatalogFile, stacToJson(stacCatalog));
  logger.info(
    { stacChildFile: stacChildFile.href, stacCatalogFile: stacCatalogFile.href },
    `STAC:ChildToCatalogUpserted`,
  );
  await upsertChildToCatalog(stacCatalogFile);
  return stacCatalogFile;
}

/**
 * Recursively found the target data collection.json from the root catalog, based on the layer name and tag.
 *
 * @param stacUrl The URL of the root STAC catalog.
 * @param layerName The name of the vector layer to find.
 * @param tag The tag of the layer to find. Or Pull Request tag like 'pull_request/pr-123', dev tag like 'dev/commit-hash' or specific version tag like 'year=2026/date=2026-02-12T02:45:08.853Z'.
 *
 * @returns Target data collection.json URL if found, otherwise throws an error.
 */
export async function getDataFromCatalog(stacUrl: URL, layerName: string, tag: string = 'latest'): Promise<URL> {
  if (stacUrl.href.endsWith('catalog.json')) {
    const catalog = await fsa.readJson<StacCatalog>(stacUrl);
    for (const link of catalog.links) {
      if (link.rel !== 'child') continue;
      // Check the collection.json of the target layer with full tag
      if (link.href.includes(`/${layerName}/${tag}/collection.json`)) {
        // Found target collection
        if (tag === 'latest') {
          // If tag is 'latest', find the derived collection data
          const collection = await fsa.readJson<StacCollection>(new URL(link.href));
          if (collection == null) {
            throw new Error(`Invalid collection at path: ${link.href}`);
          }
          if (collection.assets == null || collection.assets['parquet'] == null) {
            throw new Error(`Data asset not found in collection: ${link.href}`);
          }
          const dataAsset = collection.assets['parquet'].href;
          return new URL(dataAsset.replace(basename(dataAsset), 'collection.json'));
        } else {
          return new URL(link.href);
        }
      }
      // Check layer with partial tag category like 'pull_request', 'dev' or 'year'
      const tagPrefix = tag.split('/')[0];
      if (link.href.includes(`/${layerName}/${tagPrefix}/catalog.json`)) {
        // Recursively search in child catalog until find the full target link
        return getDataFromCatalog(new URL(link.href), layerName, tag);
      }
      // Check the root layer catalog with no tag
      if (link.href.includes(`/${layerName}/catalog.json`)) {
        return getDataFromCatalog(new URL(link.href), layerName, tag);
      }
    }
  }

  throw new Error(`Layer ${layerName} with tag ${tag} not found in catalog ${stacUrl.href}`);
}
