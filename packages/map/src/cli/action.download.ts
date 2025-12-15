import { fsa } from '@chunkd/fs';
import { CliId } from '@topographic-system/shared/src/cli.info.ts';
import { registerFileSystem } from '@topographic-system/shared/src/fs.register.ts';
import { logger } from '@topographic-system/shared/src/log.ts';
import { Url, UrlFolder } from '@topographic-system/shared/src/url.ts';
import { command, option } from 'cmd-ts';
import path, { basename } from 'path';

// Prepare a temporary folder to store the source data and processed outputs
export const tmpFolder = fsa.toUrl(path.join(process.cwd(), `tmp/${CliId}/`));

/**
 * Downloads the given source vector parquet files for processing
 */
export async function downloadFile(file: URL, location?: URL): Promise<URL> {
  logger.info({ project: file.href, downloaded: tmpFolder.href }, 'DownloadProjectFile: Start');
  try {
    const downloadFile = new URL(basename(file.pathname), location ?? tmpFolder);
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
 * Downloads the given source vector parquet files for processing
 */
export async function downloadFiles(path: URL, location?: URL): Promise<URL[]> {
  logger.info({ source: path.href, downloaded: tmpFolder.href }, 'DownloadSourceFile: Start');
  const downloadFiles = [];
  const files = await fsa.toArray(fsa.list(path));
  for (const file of files) {
    downloadFiles.push(await downloadFile(file, location));
  }
  logger.info({ destination: tmpFolder.href, number: files.length }, 'DownloadSourceFile: End');
  return downloadFiles;
}

export const downloadArgs = {
  project: option({
    type: Url,
    long: 'project',
    description: 'Path or s3 of QGIS Project to use for list map sheets.',
  }),
  source: option({
    type: UrlFolder,
    long: 'source',
    description: 'Path or s3 of source parquet vector layers to use for generate map sheets.',
  }),
};

export const downloadCommand = command({
  name: 'download',
  description: 'Download source files and project file for processing.',
  args: downloadArgs,
  async handler(args) {
    registerFileSystem();
    logger.info({ project: args.project }, 'Download: Started');

    // Download source files if not exists
    await downloadFiles(args.source);

    // Download project file if not exists
    const projectFile = await downloadFile(new URL(args.project));

    // Write outputs files to destination
    await fsa.write(fsa.toUrl('/tmp/download-path'), tmpFolder.href);
    await fsa.write(fsa.toUrl('/tmp/download-project'), projectFile.href);
    logger.info({ downloadPath: tmpFolder.href, downloadProject: projectFile.href }, 'Download: Completed');
  },
});
