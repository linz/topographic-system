import { fsa } from '@chunkd/fs';
import { Command } from '@linzjs/docker-command';
import { command, number, oneOf, option, optional, restPositionals, string } from 'cmd-ts';
import { mkdirSync } from 'fs';
import path, { basename, relative } from 'path';
import { fileURLToPath } from 'url';

import { registerFileSystem } from '../fs.register.ts';
import { logger, logId } from '../log.ts';

// Prepare a temporary folder to store the source data and processed outputs
const tmpFolder = fsa.toUrl(path.join(process.cwd(), `tmp/${logId}/`));

/**
 * Convert a path to a relative path
 *
 * @param path the path to convert
 * @param base the path to be relative to default to current path
 */
function toRelative(path: URL, base: URL = fsa.toUrl(process.cwd())): string {
  if (path.protocol !== 'file:' || base.protocol !== 'file:') throw new Error('Must be file: URL');
  return './' + relative(fileURLToPath(base), fileURLToPath(path));
}

/** Ready the json file and parse all the mapsheet code as array */
async function fromFile(file: URL): Promise<string[]> {
  const mapSheets = await fsa.readJson<string[]>(file);
  if (mapSheets == null || mapSheets.length === 0)
    throw new Error(`Invalide or empty map sheets in file: ${file.href}`);
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
        { file: file.href, size: stats?.size, ContentType: stats?.contentType, LastModified: stats?.lastModified },
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
      { file: project.href, size: stats?.size, ContentType: stats?.contentType, LastModified: stats?.lastModified },
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

export const ExportFormats = {
  Pdf: 'pdf',
  Tif: 'tif',
  GeoTif: 'geotiff',
} as const;

export type ExportFormat = (typeof ExportFormats)[keyof typeof ExportFormats];

interface ExportOptions {
  dpi: number;
  format: ExportFormat;
}

/**
 * Running python commands for qgis_export
 */
export async function qgisExport(input: URL, output: URL, mapsheets: string[], options: ExportOptions): Promise<void> {
  const cmd = Command.create('python3');

  cmd.args.push('src/python/qgis_export.py');
  cmd.args.push(toRelative(input));
  cmd.args.push(toRelative(output));
  cmd.args.push(options.format);
  cmd.args.push(options.dpi.toFixed());
  for (const mapsheet of mapsheets) cmd.args.push(mapsheet);

  const res = await cmd.run();
  logger.debug('qgis_export.py ' + cmd.args.join(' '));

  if (res.exitCode !== 0) {
    logger.fatal({ qgis_export: res }, 'Failure');
    throw new Error('qgis_export.py failed to run');
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
  format: option({
    type: oneOf([ExportFormats.Pdf, ExportFormats.Tif, ExportFormats.GeoTif]),
    long: 'format',
    description: `Export format as ${ExportFormats.Pdf}, ${ExportFormats.Tif}, or ${ExportFormats.GeoTif}`,
    defaultValue: () => ExportFormats.Pdf,
    defaultValueIsSerializable: true,
  }),
  dpi: option({
    type: number,
    long: 'dpi',
    description: 'Export dpi setting, default to 300',
    defaultValue: () => 300,
    defaultValueIsSerializable: true,
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
    // Download source files
    await downloadSourceFiles(new URL(args.source));
    // Download project file
    const downloadFile = await downloadProjectFile(new URL(args.project));

    // Prepare tmp path for the outputs
    const tempOutput = new URL('output/', tmpFolder);
    mkdirSync(tempOutput, { recursive: true });

    // Prepare all the map sheets to process
    const file = args.fromFile;
    const mapSheets = file != null ? args.mapSheet.concat(await fromFile(new URL(file))) : args.mapSheet;

    // Run python qgis export script
    const exportOptions = { dpi: args.dpi, format: args.format };
    await qgisExport(downloadFile, tempOutput, mapSheets, exportOptions);

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
  },
});
