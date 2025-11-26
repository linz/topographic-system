import { fsa } from '@chunkd/fs';
import { command, number, oneOf, option, optional, restPositionals, string } from 'cmd-ts';
import { mkdirSync } from 'fs';
import path, { basename } from 'path';
import { registerFileSystem } from '../fs.register.ts';
import { logger, logId } from '../log.ts';
import { ExportFormat, qgisExport } from '../python.runner.ts';
import { Url, UrlFolder } from '../util.ts';

// Prepare a temporary folder to store the source data and processed outputs
const tmpFolder = fsa.toUrl(path.join(process.cwd(), `tmp/${logId}/`));

/** Ready the json file and parse all the mapsheet code as array */
async function fromFile(file: URL): Promise<string[]> {
  const mapSheets = await fsa.readJson<string[]>(file);
  if (mapSheets == null || mapSheets.length === 0) {
    throw new Error(`Invalide or empty map sheets in file: ${file.href}`);
  }
  return mapSheets;
}

/**
 * Downloads the given source vector parquet files for processing
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
 * Downloads the given source vector parquet files for processing
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

function getContentType(format: ExportFormat): string {
  if (format === ExportFormats.Pdf) return 'application/pdf';
  else if (format === ExportFormats.Tif) return 'image/tiff';
  else if (format === ExportFormats.GeoTif) return 'image/tiff; application=geotiff';
  else throw new Error('Invalid format' + format);
}

export const ExportFormats = {
  Pdf: 'pdf',
  Tif: 'tif',
  GeoTif: 'geotif',
} as const;

export const ProduceArgs = {
  mapSheet: restPositionals({ type: string, displayName: 'map-sheet', description: 'Map Sheet Code to process' }),
  fromFile: option({
    type: optional(Url),
    long: 'from-file',
    description: 'Path to JSON file containing array of MapSheet Codes to Process.',
  }),
  source: option({
    type: UrlFolder,
    long: 'source',
    description: 'Path or s3 of QGIS Project to use for generate map sheets.',
  }),
  project: option({
    type: Url,
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
    description: 'Export dpi setting',
    defaultValue: () => 300,
    defaultValueIsSerializable: true,
  }),
  output: option({
    type: UrlFolder,
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
    await downloadFiles(args.source);
    // Download project file
    const projectFile = await downloadFile(new URL(args.project));

    // Prepare tmp path for the outputs
    const tempOutput = new URL('output/', tmpFolder);
    mkdirSync(tempOutput, { recursive: true });

    // Prepare all the map sheets to process
    const mapSheets = args.fromFile != null ? args.mapSheet.concat(await fromFile(args.fromFile)) : args.mapSheet;

    // Run python qgis export script
    const exportOptions = { dpi: args.dpi, format: args.format };
    await qgisExport(projectFile, tempOutput, mapSheets, exportOptions);

    // Write outputs files to destination
    for await (const file of fsa.list(tempOutput)) {
      const destPath = new URL(basename(file.pathname), args.output);
      const stream = fsa.readStream(file);
      await fsa.write(destPath, stream, {
        contentType: getContentType(args.format),
      });
      logger.info({ destPath: destPath.href }, 'Produce: FileUploaded');
    }
  },
});
