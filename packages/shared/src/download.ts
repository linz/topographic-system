import { basename } from 'node:path';
import { fileURLToPath } from 'node:url';

import { fsa } from '@chunkd/fs';
import { logger } from '@linzjs/topographic-system-shared';
import type { StacCollection, StacItem } from 'stac-ts';
import tar from 'tar';

/**
 * Download given source parquet file by given URL
 *
 * @param file - a URL pointing to a parquet file used as source data in QGIS project
 * @param target - Target to download the file to
 *
 * @returns Downloaded local file URL
 */
export async function downloadFile(file: URL, target: URL): Promise<URL> {
  const startTime = Date.now();
  logger.info({ project: file.href, downloaded: target.href, startTime }, 'DownloadProjectFile: Start');
  try {
    const downloadFile = new URL(basename(file.pathname), target);
    if (await fsa.exists(downloadFile)) return downloadFile;
    const stats = await fsa.head(file);
    logger.debug(
      { file: file.href, size: stats?.size, ContentType: stats?.contentType, LastModified: stats?.lastModified },
      'DownloadFile: stats',
    );

    const stream = fsa.readStream(file);
    if (file.href.endsWith('.parquet')) {
      await fsa.write(downloadFile, stream, {
        contentType: 'application/vnd.apache.parquet',
      });
    } else {
      await fsa.write(downloadFile, stream);
    }

    // validate file was downloaded
    if (!(await fsa.exists(downloadFile))) {
      throw new Error(`Failed to download file: ${downloadFile.href}`);
    }

    const duration = Date.now() - startTime;
    logger.info({ destination: downloadFile.href, duration }, 'DownloadFile: End');
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
export async function downloadFiles(source: URL, target: URL): Promise<URL[]> {
  logger.info({ source: source.href, downloaded: target.href }, 'DownloadSourceFile: Start');
  const downloadFiles = [];
  const files = await fsa.toArray(fsa.list(source));
  for (const file of files) {
    downloadFiles.push(await downloadFile(file, target));
  }
  logger.info({ destination: target.href, number: files.length }, 'DownloadSourceFile: End');
  return downloadFiles;
}

/**
 * Parses a Valid STAC Collection for a parquet data from s3, and download the parquet file to a tmp location for processing.
 *
 * @param collectionUrl - a URL ponting to a STAC Collection file for a parquet data
 *
 * @returns Downloaded local file URL
 */
export async function downloadFromCollection(collectionUrl: URL, target: URL): Promise<URL> {
  const sourcedCollection = await fsa.readJson<StacCollection>(new URL(collectionUrl.href));
  if (sourcedCollection == null) {
    throw new Error(`Invalid source collection at path: ${collectionUrl.href}`);
  }
  if (sourcedCollection.assets == null || sourcedCollection.assets['parquet'] == null) {
    throw new Error(`Parquet asset not found in source collection: ${collectionUrl.href}`);
  }
  const data = sourcedCollection.assets['parquet'];
  const source = new URL(data.href, collectionUrl);
  await downloadFile(source, target);
  return source;
}
/**
 *
 * @param source Where the STAC item was loaded from
 * @param stac Stac Item
 * @param target Where to store the asset files
 * @returns
 */
export async function downloadProjectFile(source: URL, stac: StacItem, target: URL): Promise<URL> {
  // Download from asset if exist
  for (const [key, asset] of Object.entries(stac.assets)) {
    if (key === 'project') {
      return await downloadFile(new URL(asset.href, source), target);
    }
  }

  // Download from project stac link if exist
  for (const link of stac.links) {
    if (link.rel === 'project') {
      const targetStac = new URL(link.href, source);
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
export async function downloadProject(source: URL, target: URL): Promise<URL> {
  logger.info({ source: source.href, downloaded: target.href }, 'Download:Start');
  const stac = await fsa.readJson<StacItem>(source);
  if (stac == null) throw new Error(`Invalid STAC Item at path: ${source.href}`);
  // Download the qgis project file
  const projectPath = await downloadProjectFile(source, stac, target);

  // Download all the assets from project
  for (const [key, asset] of Object.entries(stac.assets)) {
    if (key === 'project') continue;
    await downloadFile(new URL(asset.href), target);
  }

  const links = stac.links;
  const sources = [];
  for (const link of links) {
    if (link.rel === 'dataset' || link.rel === 'source') {
      // Download Source data
      sources.push(await downloadFromCollection(new URL(link.href, source), target));
    } else if (link.rel === 'assets') {
      // Download assets tar file and extract to tmp folder for processing
      const assetTarPath = await downloadFile(new URL(link.href, source), target);
      await tar.extract({
        file: fileURLToPath(assetTarPath),
        cwd: fileURLToPath(target),
      });
    }
  }

  if (projectPath == null) {
    throw new Error(`Project asset not found in STAC Item: ${source.href}`);
  }

  logger.info({ destination: target.href, project: projectPath.href }, 'Download: End');
  return projectPath;
}
