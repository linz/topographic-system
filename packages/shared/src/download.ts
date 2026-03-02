import path, { basename } from 'node:path';
import { fileURLToPath } from 'node:url';

import { fsa, WriteOptions } from '@chunkd/fs';
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
  const startTime = Date.now();
  logger.debug({ project: file.href, downloaded: tmpFolder.href, startTime }, 'DownloadFile:Start');
  try {
    const downloadFile = new URL(basename(file.pathname), tmpFolder);
    const [targetHead, sourceHead] = await Promise.all([fsa.head(downloadFile), fsa.head(file)]);
    if (sourceHead == null) throw new Error(`Failed to download file: ${downloadFile.href}`);
    if (targetHead != null && sourceHead != null && targetHead.size === sourceHead.size) {
      logger.info({ destination: downloadFile.href }, 'DownloadFile:Exists, skipping');
      return downloadFile;
    }

    console.log(sourceHead);

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

    const duration = Date.now() - startTime;
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
  const downloadFiles = [];
  for await (const file of fsa.list(path)) downloadFiles.push(await q(() => downloadFile(file)));
  await Promise.all(downloadFiles);
  logger.info({ destination: tmpFolder.href, number: downloadFiles.length }, 'DownloadSourceFile: End');
  return downloadFiles;
}

/**
 * Parses a Valid STAC Collection for a parquet data from s3, and download the parquet file to a tmp location for processing.
 *
 * @param collectionUrl - a URL ponting to a STAC Collection file for a parquet data
 *
 * @returns Downloaded local file URL
 */
export async function downloadFromCollection(collectionUrl: URL): Promise<URL> {
  const sourcedCollection = await fsa.readJson<StacCollection>(new URL(collectionUrl.href));
  const data = sourcedCollection.assets?.['parquet'];
  if (data == null) throw new Error(`Parquet asset not found in source collection: ${collectionUrl.href}`);
  const source = new URL(data.href);
  await downloadFile(source);
  return source;
}

export async function downloadProjectFile(stac: StacItem): Promise<URL> {
  // Download from asset if exist
  for (const [key, asset] of Object.entries(stac.assets)) {
    if (key === 'project') {
      return await downloadFile(new URL(asset.href));
    }
  }

  // Download from project stac link if exist
  for (const link of stac.links) {
    if (link.rel === 'project') {
      const stac = await fsa.readJson<StacItem>(new URL(link.href));
      if (stac == null) throw new Error(`Invalid STAC Item at path: ${link.href}`);
      return await downloadProjectFile(stac);
    }
  }

  throw new Error(`Project asset not found in STAC Item: ${stac.id}`);
}

/**
 * Parses a STAC Item for a QGIS project, determines the assets and
 * datasets used in QGIS project, and downloads them to a tmp location.
 *
 * @param path - a URL ponting to a STAC Item file for a QGIS project
 *
 * @returns an object containing two key-value pairs:
 * - `projectPath` - a URL pointing to the QGIS project file
 * - `sources` - an array of URLs pointing to the datasets used in the QGIS project
 */
export async function downloadProject(path: URL, q = pLimit(DefaultConcurrency)): Promise<URL> {
  logger.info({ source: path.href, downloaded: tmpFolder.href }, 'Download:Start');
  const stac = await fsa.readJson<StacItem>(path);
  if (stac == null) throw new Error(`Invalid STAC Item at path: ${path.href}`);
  // Download the qgis project file
  const projectPath = await downloadProjectFile(stac);

  // Download all the assets from project
  for (const [key, asset] of Object.entries(stac.assets)) {
    if (key === 'project') continue;
    await q(() => downloadFile(new URL(asset.href, path)));
  }

  const links = stac.links;
  const sources = [];
  for (const link of links) {
    if (link.rel === 'dataset' || link.rel === 'source') {
      // Download Source data
      sources.push(q(() => downloadFromCollection(new URL(link.href, path))));
    } else if (link.rel === 'assets') {
      sources.push(
        q(async () => {
          // Download assets tar file and extract to tmp folder for processing
          const assetTarPath = await downloadFile(new URL(link.href, path));
          await tar.extract({
            file: fileURLToPath(assetTarPath),
            cwd: fileURLToPath(tmpFolder),
          });
        }),
      );
    }
  }

  const results = await Promise.all(sources);

  if (projectPath == null) {
    throw new Error(`Project asset not found in STAC Item: ${path.href}`);
  }

  logger.info({ destination: tmpFolder.href, files: results.length, project: projectPath.href }, 'Download:Done');
  return projectPath;
}
