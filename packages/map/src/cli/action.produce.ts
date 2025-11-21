import { fsa } from '@chunkd/fs';
import { spawn } from 'child_process';
import { command, option, optional, restPositionals, string } from 'cmd-ts';
import { registerFileSystem } from '../fs.register.ts';
import { tmpdir } from 'os';
import { logger, logId } from '../log.ts';
import path, { basename } from 'path';
import { mkdirSync } from 'fs';
import { rm } from 'fs/promises';
import { fileURLToPath } from 'url';

// Prepare a temporary folder to store the source data and processed outputs
const tmpFolder = fsa.toUrl(path.join(tmpdir(), `${logId}/`));

/** Ready the json file and parse all the mapsheet code as array */
async function fromFile(file: URL): Promise<string[]> {
  const mapSheets = await fsa.readJson<string[]>(file);
  if (mapSheets == null || mapSheets.length == 0) throw new Error(`Invalide or empty map sheets in file: ${file.href}`);
  return mapSheets;
}

/**
 * Downloads the given source vector parquet files for processing
 */
async function downloadSourceFiles(source: URL): Promise<void> {
  logger.info({ source: source.href, downloaded: tmpFolder.href }, 'DownloadSourceFile: Start');
  try {
    const files = await fsa.toArray(fsa.list(source));
    for (const file of files) {
      const downloadFile = new URL(basename(file.pathname), tmpFolder);
      if (await fsa.exists(downloadFile)) continue;
      const stats = await fsa.head(file);
      logger.debug(
        { size: stats?.size, ContentType: stats?.contentType, LastModified: stats?.lastModified },
        'DownloadSourceFile: stats',
      );

      const stream = fsa.readStream(file);
      await fsa.write(downloadFile, stream, {
        contentType: 'application/vnd.apache.parquet',
      });
      // validate file was downloaded
      if (!(await fsa.exists(downloadFile))) {
        throw new Error(`Failed to download file: ${downloadFile.href}`);
      }

      logger.info({ downloadPath: downloadFile.href }, 'DownloadSourceFile: FileDownloaded');
    }

    logger.info({ destination: tmpFolder.href, number: files.length }, 'DownloadSourceFile: End');
  } catch (error) {
    logger.error({ source: source.href }, 'DownloadSourceFile: Error');
    throw error;
  }
}

/**
 * Downloads the given source vector parquet files for processing
 */
async function downloadProjectFile(project: URL): Promise<URL> {
  logger.info({ project: project.href, downloaded: tmpFolder.href }, 'DownloadProjectFile: Start');
  try {
    const downloadFile = new URL(basename(project.pathname), tmpFolder);
    if (await fsa.exists(downloadFile)) return downloadFile;
    const stats = await fsa.head(project);
    logger.debug(
      { size: stats?.size, ContentType: stats?.contentType, LastModified: stats?.lastModified },
      'DownloadSourceFile: stats',
    );

    const stream = fsa.readStream(project);
    await fsa.write(downloadFile, stream);
    // validate file was downloaded
    if (!(await fsa.exists(downloadFile))) {
      throw new Error(`Failed to download file: ${downloadFile.href}`);
    }

    logger.info({ destination: downloadFile.href }, 'DownloadProjectFile: End');
    return downloadFile;
  } catch (error) {
    logger.error({ project: project.href }, 'DownloadProjectFile: Error');
    throw error;
  }
}

export const ProduceArgs = {
  mapSheet: restPositionals({ type: string, displayName: 'map-sheet', description: 'Map Sheet Code to process' }),
  fromFile: option({
    type: optional(string),
    long: 'from-file',
    description: 'Path to JSON file containing array of MapSheet Codes to Process.',
  }),
  source: option({
    type: string,
    long: 'source',
    description: 'Path or s3 of QGIS Project to use for generate map sheets.',
  }),
  project: option({
    type: string,
    long: 'project',
    description: 'Path or s3 of source parquet vector layers to use for generate map sheets.',
  }),
  output: option({
    type: string,
    long: 'output',
    description: 'Path or s3 of the output directory to write generated map sheets.',
  }),
};

export const ProduceCommand = command({
  name: 'produce',
  description: 'Produce',
  args: ProduceArgs,
  async handler(args) {
    registerFileSystem();
    // Download source files to local tmp folder
    await downloadSourceFiles(new URL(args.source));
    // Download project file to local tmp folder
    const downloadFile = await downloadProjectFile(new URL(args.project));
    const tempOutput = new URL('output/', tmpFolder);
    mkdirSync(tempOutput, { recursive: true });
    // Prepare tmp path for the outputs

    // Prepare all the map sheets to process
    // const file = args.fromFile;
    // const mapSheets = file != null ? args.mapSheet.concat(await fromFile(new URL(file))) : args.mapSheet;

    const child = spawn(
      'python3',
      ['src/python/qgis_export.py', fileURLToPath(downloadFile), fileURLToPath(tempOutput), 'AZ29'],
      {
        cwd: process.cwd(),
      },
    );
    child.stdout.on('data', (data) => console.log(`stdout: ${data}`));
    child.stderr.on('data', (data) => console.log(`stderr: ${data}`));

    // Write outputs files to destination
    const output = fsa.toUrl(args.output.endsWith('/') ? args.output : args.output + '/');
    const outputFiles = await fsa.toArray(fsa.list(tempOutput));
    for (const file of outputFiles) {
      const destPath = new URL(basename(file.pathname), output);
      const stream = fsa.readStream(file);
      await fsa.write(destPath, stream, {
        contentType: 'application/pdf',
      });
      logger.info({ destPath: destPath.href }, 'Produce: FileUploaded');
    }

    // Cleanup the temporary folder once everything is done
    logger.info({ path: tmpFolder }, 'Cog:Cleanup');
    await rm(tmpFolder, { recursive: true, force: true });
  },
});
