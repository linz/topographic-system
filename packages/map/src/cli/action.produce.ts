import { fsa } from '@chunkd/fs';
import { CliId } from '@topographic-system/shared/src/cli.info.ts';
import { registerFileSystem } from '@topographic-system/shared/src/fs.register.ts';
import { logger } from '@topographic-system/shared/src/log.ts';
import { createStacCatalog, createStacLink } from '@topographic-system/shared/src/stac.ts';
import { Url, UrlFolder } from '@topographic-system/shared/src/url.ts';
import { command, number, oneOf, option, optional, restPositionals, string } from 'cmd-ts';
import { mkdirSync } from 'fs';
import path, { basename, parse } from 'path';
import type { StacCatalog, StacCollection, StacItem } from 'stac-ts';
import tar from 'tar';
import { fileURLToPath } from 'url';

import { qgisExport } from '../python.runner.ts';
import { createMapSheetStacCollection, createMapSheetStacItem } from '../stac.ts';
import { validateTiff } from '../validate.ts';
import { downloadFile } from './action.download.ts';

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
      const sourcedCollection = await fsa.readJson<StacCollection>(new URL(link.href));
      if (sourcedCollection == null) {
        throw new Error(`Invalid source collection at path: ${link.href}`);
      }
      for (const link of sourcedCollection.links) {
        if (link.rel === 'item') {
          const item = await fsa.readJson<StacItem>(new URL(link.href));
          if (item == null) {
            throw new Error(`Invalid source item at path: ${link.href}`);
          }
          const data = item.assets['parquet'];
          if (data == null) {
            throw new Error(`Parquet asset not found in source item: ${link.href}`);
          }
          const source = new URL(data.href);
          sources.push(source);
          await downloadFile(source);
        }
      }
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

// Prepare a temporary folder to store the source data and processed outputs
const tmpFolder = fsa.toUrl(path.join(process.cwd(), `tmp/${CliId}/`));

export const ExportFormats = {
  Pdf: 'pdf',
  Tiff: 'tiff',
  GeoTiff: 'geotiff',
  Png: 'png',
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
  else if (format === ExportFormats.Png) return 'image/png';
  else throw new Error(`Invalid format`);
}

export const ProduceArgs = {
  mapSheet: restPositionals({ type: string, displayName: 'map-sheet', description: 'Map Sheet Code to process' }),
  fromFile: option({
    type: optional(Url),
    long: 'from-file',
    description: 'Path to JSON file containing array of MapSheet Codes to Process.',
  }),
  project: option({
    type: Url,
    long: 'project',
    description: 'Stac Item path of QGIS Project to use for generate map sheets.',
  }),
  format: option({
    type: oneOf([ExportFormats.Pdf, ExportFormats.Tiff, ExportFormats.GeoTiff, ExportFormats.Png]),
    long: 'format',
    description: `Export format as ${ExportFormats.Pdf}, ${ExportFormats.Tiff}, ${ExportFormats.GeoTiff}, or ${ExportFormats.Png}`,
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
    // Download project file, assets, and source data from the project stac file
    const { projectPath, sources } = await downloadProject(args.project);

    // Prepare tmp path for the outputs
    const tempOutput = new URL('output/', tmpFolder);
    mkdirSync(tempOutput, { recursive: true });

    // Prepare all the map sheets to process
    const mapSheets = args.fromFile != null ? args.mapSheet.concat(await fromFile(args.fromFile)) : args.mapSheet;

    // Run python qgis export script
    const exportOptions = { dpi: args.dpi, format: args.format };
    const metadatas = await qgisExport(projectPath, tempOutput, mapSheets, exportOptions);

    // Write outputs files to destination
    const projectName = parse(args.project.pathname).name;
    for await (const file of fsa.list(tempOutput)) {
      if (args.format === ExportFormats.GeoTiff || args.format === ExportFormats.Tiff) {
        await validateTiff(file, metadatas);
      }

      const destPath = new URL(basename(file.pathname), args.output);
      const stream = fsa.readStream(file);
      await fsa.write(destPath, stream, {
        contentType: getContentType(args.format),
      });
      logger.info({ destPath: destPath.href }, 'Produce: FileUploaded');
    }

    // Create Stac Files and upload to destination
    const links = await createStacLink(sources, args.project);
    for (const metadata of metadatas) {
      const item = await createMapSheetStacItem(metadata, args.format, args.dpi, args.output, links);
      await fsa.write(new URL(`${metadata.sheetCode}.json`, args.output), JSON.stringify(item, null, 2));
    }
    const collection = createMapSheetStacCollection(metadatas, links);
    await fsa.write(new URL('collection.json', args.output), JSON.stringify(collection, null, 2));

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
