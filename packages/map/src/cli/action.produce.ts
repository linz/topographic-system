import { fsa } from '@chunkd/fs';
import { command, number, oneOf, option, optional, restPositionals, string } from 'cmd-ts';
import { mkdirSync } from 'fs';
import { basename } from 'path';

import { registerFileSystem } from '../fs.register.ts';
import { logger } from '../log.ts';
import { qgisExport } from '../python.runner.ts';
import { Url, UrlFolder } from '../util.ts';
import { downloadFile, downloadFiles, tmpFolder } from './action.download.ts';

export const ExportFormats = {
  Pdf: 'pdf',
  Tiff: 'tiff',
  GeoTiff: 'geotiff',
} as const;

export type ExportFormat = (typeof ExportFormats)[keyof typeof ExportFormats];

export interface ExportOptions {
  dpi: number;
  format: ExportFormat;
}

/** Ready the json file and parse all the mapsheet code as array */
async function fromFile(file: URL): Promise<string[]> {
  const mapSheets = await fsa.readJson<string[]>(file);
  if (mapSheets == null || mapSheets.length === 0) {
    throw new Error(`Invalide or empty map sheets in file: ${file.href}`);
  }
  return mapSheets;
}

function getContentType(format: ExportFormat): string {
  if (format === ExportFormats.Pdf) return 'application/pdf';
  else if (format === ExportFormats.Tiff) return 'image/tiff';
  else if (format === ExportFormats.GeoTiff) return 'image/tiff; application=geotiff';
  else throw new Error(`Invalid format`);
}

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
    description: 'Path or s3 of source parquet vector layers to use for generate map sheets.',
  }),
  project: option({
    type: Url,
    long: 'project',
    description: 'Path or s3 of QGIS Project to use for generate map sheets.',
  }),
  format: option({
    type: oneOf([ExportFormats.Pdf, ExportFormats.Tiff, ExportFormats.GeoTiff]),
    long: 'format',
    description: `Export format as ${ExportFormats.Pdf}, ${ExportFormats.Tiff}, or ${ExportFormats.GeoTiff}`,
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
