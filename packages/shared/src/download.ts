import { basename } from 'node:path';
import { fileURLToPath } from 'node:url';

import type { WriteOptions } from '@chunkd/fs';
import { fsa } from '@chunkd/fs';
import { HashTransform } from '@chunkd/fs/build/src/hash.stream.js';
import { qMap, qMapAll } from '@linzjs/topographic-system-shared';
import { logger } from '@linzjs/topographic-system-shared';
import pLimit from 'p-limit';
import type { StacAsset, StacCatalog, StacCollection, StacItem } from 'stac-ts';
import tar from 'tar';

export const DefaultConcurrency = 20;

/**
 * Download given source parquet file by given URL
 *
 * @param file - a URL pointing to a parquet file used as source data in QGIS project
 * @param target - Target to download the file to
 *
 * @returns Downloaded local file URL
 */
export async function downloadFile(file: URL, target: URL): Promise<URL> {
  const startTime = performance.now();
  logger.debug({ project: file.href, downloaded: target.href, startTime }, 'DownloadFile:Start');
  try {
    const downloadFile = new URL(basename(file.pathname), target);
    const [targetHead, sourceHead] = await Promise.all([fsa.head(downloadFile), fsa.head(file)]);
    if (sourceHead == null) throw new Error(`Failed to download file: ${downloadFile.href}`);
    if (targetHead != null && sourceHead != null && targetHead.size === sourceHead.size) {
      logger.info({ destination: downloadFile.href }, 'DownloadFile:Exists, skipping');
      return downloadFile;
    }

    logger.trace(
      {
        file: file.href,
        size: sourceHead.size,
        contentType: sourceHead.contentType,
        lastModified: sourceHead.lastModified,
      },
      'DownloadFile:Stats',
    );

    const fileHash = new HashTransform('sha256');
    const stream = fsa.readStream(file).pipe(fileHash);
    const meta: WriteOptions = {};
    if (file.href.endsWith('.parquet')) meta.contentType = 'application/vnd.apache.parquet';
    await fsa.write(downloadFile, stream, meta);

    const head = await fsa.head(downloadFile);
    // validate file was downloaded correctly
    if (head == null || head.size !== sourceHead?.size) {
      throw new Error(`Failed to download file: ${downloadFile.href}`);
    }

    const digest = fileHash.multihash;

    const duration = performance.now() - startTime;
    logger.info({ destination: downloadFile.href, fileHash: digest, size: head.size, duration }, 'DownloadFile:Done');
    if (downloadFile.pathname.endsWith('.tar')) {
      const startExtractTime = performance.now();
      await tar.extract({
        file: fileURLToPath(downloadFile),
        cwd: fileURLToPath(target),
      });
      logger.info(
        {
          destination: downloadFile.href,
          fileHash: digest,
          size: head.size,
          duration: performance.now() - startExtractTime,
        },
        'DownloadFile:Extract:Done',
      );
    }
    return downloadFile;
  } catch (error) {
    logger.error({ project: file.href }, 'DownloadFile: Error');
    throw error;
  }
}

/**
 * Download given source parquet file by given Folder URL, it will download all the files in the folder to a tmp location and return the list of downloaded file URLs.
 *
 * @param source - a URL pointing to a folder containing parquet files used as source data in QGIS project
 * @param target - Where to download files to
 *
 * @returns Downloaded local file URLs in an array
 */
export async function downloadFiles(path: URL, target: URL, q = pLimit(DefaultConcurrency)): Promise<URL[]> {
  logger.info({ source: path.href, downloaded: target.href }, 'DownloadSourceFile: Start');
  const files = await fsa.toArray(fsa.list(path));
  const results = await qMapAll(q, files, (file) => downloadFile(file, target));
  logger.info({ destination: target.href, number: files.length }, 'DownloadSourceFile: End');
  return results;
}

/**
 * Parses a Valid STAC Collection for a parquet data from s3, and download the parquet file to a tmp location for processing.
 *
 * @param stacUrl - a URL pointing to a STAC Collection or STAC Item file with assets
 *
 * @returns Downloaded local file URL
 */
export async function downloadAssets(
  stacUrl: URL,
  target: URL,
  filter: (asset: StacAsset) => boolean = () => true,
): Promise<URL[]> {
  const sourcedCollection = await fsa.readJson<StacCollection | StacItem>(stacUrl);
  return Promise.all(
    Object.values(sourcedCollection.assets ?? {})
      .filter(filter)
      .map((m) => {
        return downloadFile(new URL(m.href, stacUrl), target);
      }),
  );
}

/** STAC Link "rel" that should be downloaded */
const DownloadRels = new Set(['dataset', 'source', 'derived_from', 'project']);

/**
 * Parses a STAC Item for a QGIS project, determines the assets and
 * datasets used in QGIS project, and downloads them to a tmp location.
 *
 * @param source - a URL pointing to a STAC Item file for a QGIS project
 * @param target - where to download too
 *
 * @returns an object containing two key-value pairs:
 * - `projectPath` - a URL pointing to the QGIS project file
 * - `sources` - an array of URLs pointing to the datasets used in the QGIS project
 */
export async function downloadProject(projectUrl: URL, targetUrl: URL, q = pLimit(DefaultConcurrency)): Promise<URL> {
  const startTime = performance.now();
  logger.info({ source: projectUrl.href, downloaded: targetUrl.href }, 'Download:Start');
  const stac = await fsa.readJson<StacItem>(projectUrl);
  if (stac == null) throw new Error(`Invalid STAC Item at path: ${projectUrl.href}`);

  const sources: Promise<URL[] | URL>[] = [q(() => downloadAssets(projectUrl, targetUrl))];
  const downloadLinks = stac.links.filter((link) => DownloadRels.has(link.rel));
  sources.push(...qMap(q, downloadLinks, (link) => downloadAssets(new URL(link.href, projectUrl), targetUrl)));

  const results = (await Promise.all(sources)).flat();
  const projectPath = results.find((url) => url.pathname.endsWith('.qgs'));
  if (projectPath == null) throw new Error(`Unable to find project file in STAC Item: ${projectUrl.href}`);

  logger.info(
    {
      destination: targetUrl.href,
      files: results.length,
      project: projectPath.href,
      duration: performance.now() - startTime,
    },
    'Download:Done',
  );
  return projectPath;
}

const CatalogCache = new Map<string, Promise<StacCatalog>>();

function readCatalog(url: URL): Promise<StacCatalog> {
  let existing = CatalogCache.get(url.href);
  if (existing) return existing;
  existing = fsa.readJson<StacCatalog>(url);
  CatalogCache.set(url.href, existing);
  return existing;
}

/**
 * Recursively find the target data collection.json from the root catalog,
 *
 * @param stacUrl The URL of the root STAC catalog.
 * @param layerName The name of the vector layer to find.
 *
 * @returns Target data collection.json URL if found, otherwise throws an error.
 */
export async function getDataFromCatalog(stacUrl: URL, layerName: string): Promise<URL> {
  const catalog = await readCatalog(stacUrl);

  const targetLayer = `/${layerName}/catalog.json`;
  const catLink = catalog.links.find((link) => link.href.endsWith(targetLayer));
  if (catLink) {
    const catUrl = new URL(catLink.href, stacUrl); // /data/airport/catalog.json
    return new URL('latest/collection.json', catUrl); // /data/airport/latest/collection.json
  }

  const dataLink = catalog.links.find((link) => link.href.endsWith('/data/catalog.json'));
  if (dataLink) return getDataFromCatalog(new URL(dataLink.href, stacUrl), layerName);

  throw new Error(`Layer ${layerName} not found in catalog ${stacUrl.href}`);
}
