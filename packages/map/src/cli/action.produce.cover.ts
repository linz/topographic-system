import { fsa } from '@chunkd/fs';
import { CliId } from '@topographic-system/shared/src/cli.info.ts';
import { downloadFile, downloadFromCollection } from '@topographic-system/shared/src/download.ts';
import { registerFileSystem } from '@topographic-system/shared/src/fs.register.ts';
import { logger } from '@topographic-system/shared/src/log.ts';
import { createStacCatalog, createStacItem, createStacLink } from '@topographic-system/shared/src/stac.factory.ts';
import { Url, UrlFolder } from '@topographic-system/shared/src/url.ts';
import { command, flag, number, oneOf, option, optional, restPositionals, string } from 'cmd-ts';
import { parse } from 'path';
import type { StacCatalog, StacItem } from 'stac-ts';

import { listMapSheets } from '../python.runner.ts';
import { createMapSheetStacCollection, type MapSheetStacItem } from '../stac.ts';
import { fromFile } from './action.produce.ts';
import { map } from 'zod';

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

const ProduceArgs = {
  mapSheet: restPositionals({ type: string, displayName: 'map-sheet', description: 'Map Sheet Code to process' }),
  fromFile: option({
    type: optional(Url),
    long: 'from-file',
    description: 'Path to JSON file containing array of MapSheet Codes to Process.',
  }),
  all: flag({
    long: 'all',
    description: 'Process all map sheets in the project.',
    defaultValue: () => false,
    defaultValueIsSerializable: true,
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

export const produceCoverCommand = command({
  name: 'produce-cover',
  description: 'Read a Qgis project and mapsheet data, then generate stac files for the exports.',
  args: ProduceArgs,
  async handler(args) {
    registerFileSystem();
    logger.info({ project: args.project }, 'ProduceCover: Started');

    const mapSheets = args.fromFile != null ? args.mapSheet.concat(await fromFile(args.fromFile)) : args.mapSheet;

    // Download mapshseet layer data from the project stac file
    const stac = await fsa.readJson<StacItem>(args.project);
    if (stac == null) throw new Error(`Invalid STAC Item at path: ${args.project.href}`);
    for (const link of stac.links) {
      if (link.rel === 'dataset' && link.href.includes(args.mapSheetLayer)) {
        await downloadFromCollection(new URL(link.href));
      }
    }
    // Download project file from the project stac file
    let projectPath;
    for (const [key, asset] of Object.entries(stac.assets)) {
      const downloadedPath = await downloadFile(new URL(asset.href));
      if (key === 'project') projectPath = downloadedPath;
    }
    if (projectPath == null) {
      throw new Error(`Project asset not found in STAC Item: ${args.project.href}`);
    }

    // Run python list all the mapsheet covering metadata
    const exportOptions = {
      layout: args.layout,
      mapSheetLayer: args.mapSheetLayer,
      dpi: args.dpi,
      format: args.format,
    };

    const metadatas = await listMapSheets(projectPath, exportOptions, args.all ? undefined : mapSheets);

    // Create Stac Files and upload to destination
    const derivedProjectLink = stac.links.find((link) => link.rel === 'derived_from');
    const sources = stac.links.filter((link) => link.rel === 'dataset').map((link) => new URL(link.href));
    const links = createStacLink(sources, derivedProjectLink ? new URL(derivedProjectLink.href) : args.project);
    for (const metadata of metadatas) {
      const item = createStacItem(metadata.sheetCode, links, {}, metadata.geometry, metadata.bbox) as MapSheetStacItem;
      item.properties['proj:epsg'] = metadata.epsg;
      item.properties['linz_topographic_system:options'] = {
        mapsheet: metadata.sheetCode,
        format: args.format,
        dpi: args.dpi,
      };
      await fsa.write(new URL(`${metadata.sheetCode}.json`, args.output), JSON.stringify(item, null, 2));
    }
    const collection = createMapSheetStacCollection(metadatas, links);
    await fsa.write(new URL('collection.json', args.output), JSON.stringify(collection, null, 2));

    const projectName = parse(args.project.pathname).name;
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
