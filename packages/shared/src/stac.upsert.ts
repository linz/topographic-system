import { basename } from 'node:path';
import { isDeepStrictEqual } from 'node:util';

import { fsa } from '@chunkd/fs';
import type { SpatialExtent, StacCatalog, StacCollection, StacItem, StacLink, TemporalExtent } from 'stac-ts';

import { CliDate } from './cli.info.ts';
import { logger } from './log.ts';
import type { ColumnStats } from './parquet.metadata.ts';
import { extractSpatialExtent, extractTemporalExtent } from './parquet.metadata.ts';
import {
  createStacAssetFromFileName,
  createStacCatalogFromFilename,
  createStacCollectionFromFileName,
  createStacItemFromFileName,
  stacToJson,
} from './stac.factory.ts';
import { addChildDataToParent, addParentDataToChild, compareStacAssets } from './stac.links.ts';

export function createCollectionExtentFromParquet(
  bbox: SpatialExtent,
  dates: TemporalExtent,
): StacCollection['extent'] {
  return { spatial: { bbox: [bbox] }, temporal: { interval: [dates] } };
}

/**
 * Checks whether the extent of a STAC Collection has changed, with a given precision to avoid unnecessary updates due to minor floating point differences.
 * @param currentExtent
 * @param nextExtent
 * @param precision Number of decimal places to consider when comparing the spatial extent bbox. Defaults to 8.
 */
export function hasCollectionExtentChanged(
  currentExtent: StacCollection['extent'],
  nextExtent: StacCollection['extent'],
  precision: number = 8,
): boolean {
  const roundBbox = (extent: StacCollection['extent']): StacCollection['extent'] => ({
    ...extent,
    spatial: {
      ...extent.spatial,
      bbox: extent.spatial.bbox.map((bbox) =>
        bbox.map((coord) => Number(coord.toFixed(precision))),
      ) as StacCollection['extent']['spatial']['bbox'],
    },
  });
  return !isDeepStrictEqual(roundBbox(currentExtent), roundBbox(nextExtent));
}

/**
 * Given a data asset path, create or update the corresponding STAC Item with the asset information.
 * Note:
 * One STAC Item may contain multiple assets.
 * Running this function multiple times with different assets for the same dataset will update the same STAC Item.
 * Running this in parallel for different assets of the same dataset may lead to race conditions,
 * as STAC Collection and Catalogs up to the RootCatalog are updated.
 *
 * @param rootCatalog - The URL of the root catalog to which the STAC Collection belongs. This is used to ensure that the correct parent-child relationships are maintained when creating or updating the STAC Collection and its parent Catalogs.
 * @param assetFile - The URL of the data asset to be added to the STAC Item.
 * @param stacItemFile - Optional URL of the STAC Item file. If not provided, it will be derived from the data asset path.
 *
 * @returns The updated or newly created STAC Item, which includes the new asset and has been saved to s3.
 * */
export async function upsertAssetToItem(rootCatalog: URL, assetFile: URL, stacItemFile?: URL): Promise<URL> {
  const extension = assetFile.href.split('.').pop() ?? '';
  if (extension === 'json') {
    logger.warn({ asset: assetFile.href }, 'STAC:UpsertAssetToItemSkippedForJson');
    return assetFile;
  }
  const dataset = basename(assetFile.href, `.${extension}`);
  const stacAsset = await createStacAssetFromFileName(assetFile);
  if (stacItemFile == null) {
    stacItemFile = new URL(`./${dataset}.json`, assetFile);
  }
  const stacItem = await createStacItemFromFileName(rootCatalog, stacItemFile);
  if (compareStacAssets(stacItem.assets[extension], stacAsset)) {
    logger.info({ dataset, asset: assetFile.href, stacItem: stacItemFile.href }, 'STAC:AssetInItemAlreadyUpToDate');
    return stacItemFile;
  }
  stacItem.assets[extension] = stacAsset;
  stacItem.properties.datetime = CliDate;
  await fsa.write(stacItemFile, stacToJson(stacItem));
  logger.info({ dataset, asset: assetFile.href, stacItem: stacItemFile.href }, 'STAC:AssetInItemAddedOrUpdated');
  await upsertItemToCollection(rootCatalog, stacItemFile);
  return stacItemFile;
}

/**
 * Given a data asset path, create or update the corresponding STAC Catalog with the asset information.
 * Note:
 * One STAC Collection per geoparquet file, with additional related assets.
 *
 * @param rootCatalog - The URL of the root catalog to which the STAC Collection belongs. This is used to ensure that the correct parent-child relationships are maintained when creating or updating the STAC Collection and its parent Catalogs.
 * @param assetFile - The URL of the data asset to be added to the STAC Item.
 * @param replaceExtraLinks - Any additional links to be added to the Collection, e.g. derived_from.
 *
 * @param stacCollectionFile - Optional URL of the STAC Item file. If not provided, it will be derived from the data asset path.
 * @returns The updated or newly created STAC Item, which includes the new asset and has been saved to s3.
 * */
export async function upsertAssetToCollection(
  rootCatalog: URL,
  assetFile: URL,
  replaceExtraLinks: StacLink[] = [],
  stacCollectionFile: URL = new URL(`./collection.json`, assetFile),
): Promise<URL> {
  const extension = assetFile.href.split('.').pop() ?? '';
  const dataset = basename(assetFile.href, `.${extension}`);
  const stacAsset = await createStacAssetFromFileName(assetFile);
  const stacCollection = await createStacCollectionFromFileName(rootCatalog, stacCollectionFile);
  stacCollection['assets'] = stacCollection['assets'] ?? {};
  if (compareStacAssets(stacCollection.assets['data'], stacAsset)) {
    logger.info(
      { dataset, asset: assetFile.href, stacCollection: stacCollectionFile.href },
      'STAC:AssetInCollectionAlreadyUpToDate',
    );
    return stacCollectionFile;
  }
  stacCollection.assets[extension] = stacAsset;
  if (extension === 'parquet') {
    const bbox = extractSpatialExtent(stacAsset['table:columns'] as ColumnStats[]);
    const dates = extractTemporalExtent(stacAsset['table:columns'] as ColumnStats[]);
    const nextExtent = createCollectionExtentFromParquet(bbox, dates);
    if (hasCollectionExtentChanged(stacCollection.extent, nextExtent)) {
      stacCollection.extent = nextExtent;
    }
  }

  if (replaceExtraLinks.length > 0) {
    const extraRels = new Set(replaceExtraLinks.map((l) => l.rel));
    stacCollection.links = stacCollection.links.filter((l) => !extraRels.has(l.rel));
    stacCollection.links.push(...replaceExtraLinks);
  }

  stacCollection['updated'] = CliDate;

  await fsa.write(stacCollectionFile, stacToJson(stacCollection));
  logger.info(
    { dataset, asset: assetFile.href, stacCollection: stacCollectionFile.href },
    'STAC:AssetInCollectionAddedOrUpdated',
  );
  await upsertChildToCatalog(rootCatalog, stacCollectionFile);
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
  rootCatalog: URL,
  stacItemFile: URL,
  stacCollectionFile: URL = new URL('./collection.json', stacItemFile),
): Promise<URL> {
  let [stacCollection, stacItem] = await Promise.all([
    createStacCollectionFromFileName(rootCatalog, stacCollectionFile),
    createStacItemFromFileName(rootCatalog, stacItemFile),
  ]);
  if (stacItemFile.href === stacCollectionFile.href) {
    logger.error({ stacItemFile: stacItemFile.href }, 'STAC:ItemAndCollectionSameFileError');
    throw new Error(`STAC Item file and Collection file cannot be the same: ${stacItemFile.href}`);
  }

  stacItem = addParentDataToChild(stacItem, stacCollection) as StacItem;
  stacCollection = addChildDataToParent(stacCollection, stacItem) as StacCollection;
  await Promise.all([
    fsa.write(stacItemFile, stacToJson(stacItem)),
    fsa.write(stacCollectionFile, stacToJson(stacCollection)),
  ]);

  logger.info({ stacCollectionFile: stacCollectionFile.href }, 'ToParquet:STACItemToCollectionUpserted');
  await upsertChildToCatalog(rootCatalog, stacCollectionFile);

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
  rootCatalog: URL,
  stacChildFile: URL,
  stacCatalogFile: URL = new URL('../catalog.json', stacChildFile),
): Promise<URL> {
  if (stacChildFile.href === rootCatalog.href) {
    logger.info({ stacChildFile: stacChildFile.href }, `STAC:ReachedRootCatalog`);
    return stacChildFile;
  }
  const childIsCollection = basename(stacChildFile.href) === 'collection.json';
  let stacChild = childIsCollection
    ? await createStacCollectionFromFileName(rootCatalog, stacChildFile)
    : await createStacCatalogFromFilename(rootCatalog, stacChildFile);
  let stacCatalog = await createStacCatalogFromFilename(rootCatalog, stacCatalogFile);
  stacChild = addParentDataToChild(stacChild, stacCatalog) as StacCollection | StacCatalog;
  stacCatalog = addChildDataToParent(stacCatalog, stacChild) as StacCatalog;

  await Promise.all([
    fsa.write(stacChildFile, stacToJson(stacChild)),
    fsa.write(stacCatalogFile, stacToJson(stacCatalog)),
  ]);
  logger.info(
    { stacChildFile: stacChildFile.href, stacCatalogFile: stacCatalogFile.href },
    `STAC:ChildToCatalogUpserted`,
  );
  await upsertChildToCatalog(rootCatalog, stacCatalogFile);
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
          const collectionUrl = new URL(link.href, stacUrl);
          // If tag is 'latest', find the derived collection data
          const collection = await fsa.readJson<StacCollection>(collectionUrl);
          if (collection == null) {
            throw new Error(`Invalid collection at path: ${link.href}`);
          }
          if (collection.assets == null || collection.assets['parquet'] == null) {
            throw new Error(`Data asset not found in collection: ${link.href}`);
          }
          // TODO we should be looking for the "asset" type not the asset named "parquet"
          const dataAsset = collection.assets['parquet'].href;
          // TODO this logic should really look for the "latest-version" record
          // https://github.com/stac-extensions/version
          return new URL(dataAsset.replace(basename(dataAsset), 'collection.json'), collectionUrl);
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
