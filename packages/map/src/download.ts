import { fsa } from '@chunkd/fs';
import { CliId } from '@topographic-system/shared/src/cli.info.ts';
import { logger } from '@topographic-system/shared/src/log.ts';
import path, { basename } from 'path';
import type { StacCollection, StacItem } from 'stac-ts';
import tar from 'tar';
import { fileURLToPath } from 'url';

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
  logger.info({ project: file.href, downloaded: tmpFolder.href }, 'DownloadProjectFile: Start');
  try {
    const downloadFile = new URL(basename(file.pathname), tmpFolder);
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

    logger.info({ destination: downloadFile.href }, 'DownloadFile: End');
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
export async function downloadFiles(path: URL): Promise<URL[]> {
  logger.info({ source: path.href, downloaded: tmpFolder.href }, 'DownloadSourceFile: Start');
  const downloadFiles = [];
  const files = await fsa.toArray(fsa.list(path));
  for (const file of files) {
    downloadFiles.push(await downloadFile(file));
  }
  logger.info({ destination: tmpFolder.href, number: files.length }, 'DownloadSourceFile: End');
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
  if (sourcedCollection == null) {
    throw new Error(`Invalid source collection at path: ${collectionUrl.href}`);
  }
  if (sourcedCollection.assets == null || sourcedCollection.assets['parquet'] == null) {
    throw new Error(`Parquet asset not found in source collection: ${collectionUrl.href}`);
  }
  const data = sourcedCollection.assets['parquet'];
  const source = new URL(data.href);
  await downloadFile(source);
  return source;
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
export async function downloadProject(path: URL): Promise<{ projectPath: URL; sources: URL[] }> {
  logger.info({ source: path.href, downloaded: tmpFolder.href }, 'Download: Start');
  const stac = await fsa.readJson<StacItem>(path);
  if (stac == null) throw new Error(`Invalid STAC Item at path: ${path.href}`);

  let projectPath;
  for (const [key, asset] of Object.entries(stac.assets)) {
    const downloadedPath = await downloadFile(new URL(asset.href));
    if (key === 'project') projectPath = downloadedPath;
  }

  if (projectPath == null) {
    throw new Error(`Project asset not found in STAC Item: ${path.href}`);
  }

  const links = stac.links;
  const sources = [];
  for (const link of links) {
    if (link.rel === 'dataset') {
      // Download Source data
      sources.push(await downloadFromCollection(new URL(link.href)));
    } else if (link.rel === 'assets') {
      // Download assets tar file and extract to tmp folder for processing
      const assetTarPath = await downloadFile(new URL(link.href));
      await tar.extract({
        file: fileURLToPath(assetTarPath),
        cwd: fileURLToPath(tmpFolder),
      });
    }
  }

  logger.info({ destination: tmpFolder.href, project: projectPath.href }, 'Download: End');
  return { projectPath, sources };
}
