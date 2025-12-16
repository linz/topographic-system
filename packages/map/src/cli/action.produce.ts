import { fsa } from '@chunkd/fs';
import { CliId } from '@topographic-system/shared/src/cli.info.ts';
import { registerFileSystem } from '@topographic-system/shared/src/fs.register.ts';
import { logger } from '@topographic-system/shared/src/log.ts';
import { createStacCatalog, createStacLink } from '@topographic-system/shared/src/stac.ts';
import { Url, UrlFolder } from '@topographic-system/shared/src/url.ts';
import { command, number, oneOf, option, optional, restPositionals, string } from 'cmd-ts';
import { mkdirSync } from 'fs';
import path, { basename, parse } from 'path';
import type { StacCatalog } from 'stac-ts';

import { qgisExport } from '../python.runner.ts';
import { createMapSheetStacCollection, createMapSheetStacItem } from '../stac.ts';
import { validateTiff } from '../validate.ts';
import { downloadFile, downloadFiles } from './action.download.ts';

// Prepare a temporary folder to store the source data and processed outputs
const tmpFolder = fsa.toUrl(path.join(process.cwd(), `tmp/${CliId}/`));

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

export function getContentType(format: ExportFormat): string {
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
    const projectFile = await downloadFile(args.project);

    // Prepare tmp path for the outputs
    const tempOutput = new URL('output/', tmpFolder);
    mkdirSync(tempOutput, { recursive: true });

    // Prepare all the map sheets to process
    const mapSheets = args.fromFile != null ? args.mapSheet.concat(await fromFile(args.fromFile)) : args.mapSheet;

    // Run python qgis export script
    const exportOptions = { dpi: args.dpi, format: args.format };
    const metadatas = await qgisExport(projectFile, tempOutput, mapSheets, exportOptions);

    // Write outputs files to destination
    const projectName = parse(args.project.pathname).name;
    const outputUrl = new URL(`/${projectName}/${CliId}/`, args.output);
    for await (const file of fsa.list(tempOutput)) {
      if (args.format === ExportFormats.GeoTiff || args.format === ExportFormats.Tiff) {
        await validateTiff(file, metadatas);
      }

      const destPath = new URL(basename(file.pathname), outputUrl);
      const stream = fsa.readStream(file);
      await fsa.write(destPath, stream, {
        contentType: getContentType(args.format),
      });
      logger.info({ destPath: destPath.href }, 'Produce: FileUploaded');
    }

    // Create Stac Files and upload to destination
    const links = await createStacLink(args.source, args.project);
    for (const metadata of metadatas) {
      const item = await createMapSheetStacItem(metadata, args.format, args.dpi, outputUrl, links);
      await fsa.write(new URL(`${metadata.sheetCode}.json`, outputUrl), JSON.stringify(item, null, 2));
    }
    const collection = createMapSheetStacCollection(metadatas, links);
    await fsa.write(new URL('collection.json', outputUrl), JSON.stringify(collection, null, 2));

    const catalogPath = new URL(`/${projectName}/catalog.json`, args.output);
    const title = 'Topographic System Map Producer';
    const description =
      'Topographic System Map Producer to generate maps from Qgis project in pdf, tiff, geotiff formats';
    const catalogLinks = [
      {
        rel: 'collection',
        href: `./${CliId}/collection.json`,
        type: 'application/json',
      },
    ];
    let catalog = createStacCatalog(title, description, catalogLinks);
    const existing = await fsa.exists(catalogPath);
    if (existing) {
      catalog = await fsa.readJson<StacCatalog>(catalogPath);
      catalog.links.push({
        rel: 'collection',
        href: `./${CliId}/collection.json`,
        type: 'application/json',
      });
    }
    await fsa.write(catalogPath, JSON.stringify(catalog, null, 2));
  },
});
