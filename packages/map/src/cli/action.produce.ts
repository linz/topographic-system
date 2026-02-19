import { fsa } from '@chunkd/fs';
import { CliId } from '@topographic-system/shared/src/cli.info.ts';
import { downloadProject, tmpFolder } from '@topographic-system/shared/src/download.ts';
import { registerFileSystem } from '@topographic-system/shared/src/fs.register.ts';
import { logger } from '@topographic-system/shared/src/log.ts';
import { createStacCatalog, createStacLink } from '@topographic-system/shared/src/stac.factory.ts';
import { Url, UrlFolder } from '@topographic-system/shared/src/url.ts';
import { command, number, oneOf, option, optional, restPositionals, string } from 'cmd-ts';
import { mkdirSync } from 'fs';
import { basename, parse } from 'path';
import type { StacCatalog, StacItem } from 'stac-ts';

import { qgisExport } from '../python.runner.ts';
import { createMapSheetStacCollection, createMapSheetStacItem } from '../stac.ts';
import { validateTiff } from '../validate.ts';

export const ExportFormats = {
  Pdf: 'pdf',
  Tiff: 'tiff',
  GeoTiff: 'geotiff',
  Png: 'png',
} as const;

export type ExportFormat = (typeof ExportFormats)[keyof typeof ExportFormats];

export interface ExportOptions {
  layout: string;
  mapSheetLayer: string;
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
  layout: option({
    type: string,
    long: 'layout',
    description: 'Qgis Layout name to use for export',
    defaultValue: () => 'tiff-50',
    defaultValueIsSerializable: true,
  }),
  mapSheetLayer: option({
    type: string,
    long: 'map-sheet-layer',
    description: 'Qgis Map Sheet Layer name to use for export',
    defaultValue: () => 'nz_topo50_map_sheet',
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
    const exportOptions = {
      layout: args.layout,
      mapSheetLayer: args.mapSheetLayer,
      dpi: args.dpi,
      format: args.format,
    };
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
    const stac = await fsa.readJson<StacItem>(args.project);
    const derivedProjectLink = stac.links.find((link) => link.rel === 'derived_from');
    const links = createStacLink(sources, derivedProjectLink ? new URL(derivedProjectLink.href) : args.project);
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
