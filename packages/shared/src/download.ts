import { basename } from 'node:path';
import { fileURLToPath } from 'node:url';

import type { WriteOptions } from '@chunkd/fs';
import { fsa } from '@chunkd/fs';
import { HashTransform } from '@chunkd/fs/build/src/hash.stream.js';
import { logger } from '@linzjs/topographic-system-shared';
import pLimit from 'p-limit';
import type { StacCollection, StacItem } from 'stac-ts';
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
  const pendingDownloads: Promise<URL>[] = [];
  for await (const file of fsa.list(path)) pendingDownloads.push(q(() => downloadFile(file, target)));
  const results = await Promise.all(pendingDownloads);
  logger.info({ destination: target.href, number: pendingDownloads.length }, 'DownloadSourceFile: End');
  return results;
}

/**
 * Parses a Valid STAC Collection for a parquet data from s3, and download the parquet file to a tmp location for processing.
 *
 * @param collectionUrl - a URL pointing to a STAC Collection file for a parquet data
 *
 * @returns Downloaded local file URL
 */
export async function downloadFromCollection(collectionUrl: URL, target: URL): Promise<URL> {
  const sourcedCollection = await fsa.readJson<StacCollection>(new URL(collectionUrl.href));
  const data = sourcedCollection.assets?.['parquet'];
  if (data == null) throw new Error(`Parquet asset not found in source collection: ${collectionUrl.href}`);
  return downloadFile(new URL(data.href, collectionUrl), target);
}

export async function downloadProjectFile(sourceUrl: URL, stac: StacItem, target: URL): Promise<URL> {
  // Download from asset if exist
  for (const [key, asset] of Object.entries(stac.assets)) {
    if (key === 'project') {
      return await downloadFile(new URL(asset.href, sourceUrl), target);
    }
  }

  // Download from project stac link if exist
  for (const link of stac.links) {
    if (link.rel === 'project') {
      const targetStac = new URL(link.href, sourceUrl);
      const projectStac = await fsa.readJson<StacItem>(targetStac);
      if (projectStac == null) throw new Error(`Invalid STAC Item at path: ${link.href}`);
      return await downloadProjectFile(targetStac, projectStac, target);
    }
  }

  throw new Error(`Project asset not found in STAC Item: ${stac.id}`);
}

/**
 * Parses a STAC Item for a QGIS project, determines the assets and
 * datasets used in QGIS project, and downloads them to a tmp location.
 *
 * @param source - a URL ponting to a STAC Item file for a QGIS project
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
  // Download the qgis project file
  const projectPath = await downloadProjectFile(projectUrl, stac, targetUrl);

  if (projectPath == null) {
    throw new Error(`Project asset not found in STAC Item: ${projectUrl.href}`);
  }

  const sources: Promise<URL>[] = [];

  // Download all the assets from project
  for (const [key, asset] of Object.entries(stac.assets)) {
    if (key === 'project') continue;
    sources.push(q(() => downloadFile(new URL(asset.href, projectUrl), targetUrl)));
  }

  const links = stac.links;
  for (const link of links) {
    if (link.rel === 'dataset' || link.rel === 'source') {
      // Download Source data
      sources.push(q(() => downloadFromCollection(new URL(link.href, projectUrl), targetUrl)));
    } else if (link.rel === 'assets') {
      sources.push(
        q(async () => {
          // Download assets tar file and extract to tmp folder for processing
          const assetTarPath = await downloadFile(new URL(link.href, projectUrl), targetUrl);
          await tar.extract({
            file: fileURLToPath(assetTarPath),
            cwd: fileURLToPath(targetUrl),
          });
          return assetTarPath;
        }),
      );
    }
  }

  const results = await Promise.all(sources);

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
