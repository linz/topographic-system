import path, { basename } from 'node:path';
import { fileURLToPath } from 'node:url';

import { fsa } from '@chunkd/fs';
import type { WriteOptions } from '@chunkd/fs/build/src/file.system.js';
import { HashTransform } from '@chunkd/fs/build/src/hash.stream.js';
import { CliId, logger } from '@linzjs/topographic-system-shared';
import pLimit from 'p-limit';
import type { StacCollection, StacItem } from 'stac-ts';
import tar from 'tar';

export const DefaultConcurrency = 20;

// Prepare a temporary folder to store the source data and processed outputs
export const tmpFolder = fsa.toUrl(path.join(process.cwd(), `tmp/${CliId}/`));

/**
 * Download given source parquet file by given URL
 *
 * @param file - a URL ponting to a parquet file used as source data in QGIS project
 *
 * @returns Downloaded local file URL
 */
export async function downloadFile(file: URL): Promise<URL> {
  const startTime = performance.now();
  logger.debug({ project: file.href, downloaded: tmpFolder.href, startTime }, 'DownloadFile:Start');
  try {
    const downloadFile = new URL(basename(file.pathname), tmpFolder);
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
 * @param path - a URL pointing to a folder containing parquet files used as source data in QGIS project
 *
 * @returns Downloaded local file URLs in an array
 */
export async function downloadFiles(path: URL, q = pLimit(DefaultConcurrency)): Promise<URL[]> {
  logger.info({ source: path.href, downloaded: tmpFolder.href }, 'DownloadSourceFile: Start');
  const downloadFiles: Promise<URL>[] = [];
  for await (const file of fsa.list(path)) downloadFiles.push(q(() => downloadFile(file)));
  const results = await Promise.all(downloadFiles);
  logger.info({ destination: tmpFolder.href, number: downloadFiles.length }, 'DownloadSourceFile: End');
  return results;
}

/**
 * Parses a Valid STAC Collection for a parquet data from s3, and download the parquet file to a tmp location for processing.
 *
 * @param collectionUrl - a URL pointing to a STAC Collection file for a parquet data
 *
 * @returns Downloaded local file URL
 */
export async function downloadFromCollection(collectionUrl: URL): Promise<URL> {
  const sourcedCollection = await fsa.readJson<StacCollection>(new URL(collectionUrl.href));
  const data = sourcedCollection.assets?.['parquet'];
  if (data == null) throw new Error(`Parquet asset not found in source collection: ${collectionUrl.href}`);
  return downloadFile(new URL(data.href, collectionUrl));
}

export async function downloadProjectFile(stac: StacItem, sourceUrl: URL): Promise<URL> {
  // Download from asset if exist
  for (const [key, asset] of Object.entries(stac.assets)) {
    if (key === 'project') {
      return await downloadFile(new URL(asset.href, sourceUrl));
    }
  }

  // Download from project stac link if exist
  for (const link of stac.links) {
    if (link.rel === 'project') {
      const targetUrl = new URL(link.href, sourceUrl);
      const stac = await fsa.readJson<StacItem>(targetUrl);
      if (stac == null) throw new Error(`Invalid STAC Item at path: ${link.href}`);
      return await downloadProjectFile(stac, targetUrl);
    }
  }

  throw new Error(`Project asset not found in STAC Item: ${stac.id}`);
}

/**
 * Parses a STAC Item for a QGIS project, determines the assets and
 * datasets used in QGIS project, and downloads them to a tmp location.
 *
 * @param projectUrl - a URL pointing to a STAC Item file for a QGIS project
 *
 * @returns an object containing two key-value pairs:
 * - `projectPath` - a URL pointing to the QGIS project file
 * - `sources` - an array of URLs pointing to the datasets used in the QGIS project
 */
export async function downloadProject(projectUrl: URL, q = pLimit(DefaultConcurrency)): Promise<URL> {
  const startTime = performance.now();
  logger.info({ source: projectUrl.href, downloaded: tmpFolder.href }, 'Download:Start');
  const stac = await fsa.readJson<StacItem>(projectUrl);
  if (stac == null) throw new Error(`Invalid STAC Item at path: ${projectUrl.href}`);
  // Download the qgis project file
  const projectPath = await downloadProjectFile(stac, projectUrl);

  if (projectPath == null) {
    throw new Error(`Project asset not found in STAC Item: ${projectUrl.href}`);
  }

  const sources: Promise<URL>[] = [];

  // Download all the assets from project
  for (const [key, asset] of Object.entries(stac.assets)) {
    if (key === 'project') continue;
    sources.push(q(() => downloadFile(new URL(asset.href, projectUrl))));
  }

  const links = stac.links;
  for (const link of links) {
    if (link.rel === 'dataset' || link.rel === 'source') {
      // Download Source data
      sources.push(q(() => downloadFromCollection(new URL(link.href, projectUrl))));
    } else if (link.rel === 'assets') {
      sources.push(
        q(async () => {
          // Download assets tar file and extract to tmp folder for processing
          const assetTarPath = await downloadFile(new URL(link.href, projectUrl));
          await tar.extract({
            file: fileURLToPath(assetTarPath),
            cwd: fileURLToPath(tmpFolder),
          });
          return assetTarPath;
        }),
      );
    }
  }

  const results = await Promise.all(sources);

  logger.info(
    {
      destination: tmpFolder.href,
      files: results.length,
      project: projectPath.href,
      duration: performance.now() - startTime,
    },
    'Download:Done',
  );
  return projectPath;
}
