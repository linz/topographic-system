import { basename } from 'path';

import { fsa } from '@chunkd/fs';
import {
  downloadFile,
  getDataFromCatalog,
  isArgo,
  logger,
  registerFileSystem,
  Url,
  UrlFolder,
} from '@linzjs/topographic-system-shared';
import { downloadAssets } from '@linzjs/topographic-system-shared/src/download.ts';
import { StacCollectionWriter, StacUpdater } from '@linzjs/topographic-system-stac';
import { StorageStrategyOption } from '@linzjs/topographic-system-stac/src/parser.ts';
import { command, flag, number, oneOf, option, optional, restPositionals, string } from 'cmd-ts';
import type { StacCollection, StacItem } from 'stac-ts';

import { qFromArgs } from '../limit.ts';
import { pyRunner } from '../python.runner.ts';
import { type ExportOptions } from '../stac.ts';
import { fromFile } from './action.produce.ts';
import { tempLocation } from './shared.args.ts';

export const ExportFormats = {
  Pdf: 'pdf',
  Tiff: 'tiff',
  GeoTiff: 'geotiff',
  Png: 'png',
} as const;

export type ExportFormat = (typeof ExportFormats)[keyof typeof ExportFormats];

interface dataTag {
  layer: string;
  tag: string;
}

/**
 * Parse a input data tag string into an array of dataTag,
 * For example: "airport/pull_request/pr-18/,contours/pull_request/pr-18/" => [{ layer: "airport", tag: "pull_request/pr-18/" }, { layer: "contours", tag: "pull_request/pr-18/" }]
 */
export function parseDataTag(input: string): dataTag[] {
  const tags: dataTag[] = [];
  const pairs = input.split(',').map((part) => part.trim());
  const error = `Invalid data tag format, expected "layer/latest", "layer/pull_request/pr-<number>", or "layer/year/<date>", got ${input}`;
  for (const rawPair of pairs) {
    // Remove leading and trailing slashes
    const pair = rawPair.replace(/^\/+|\/+$/g, '');
    const splits = pair.split('/');

    if (splits.length === 2) {
      // If only one tag provided, it should be always 'latest' tag, for example "airport/latest/"
      if (splits[1] !== 'latest') {
        throw new Error(error);
      }
      tags.push({ layer: splits[0]!, tag: 'latest' });
    } else if (splits.length === 3) {
      // Other tags like pull request or date tags should have 3 parts, for example "airport/pull_request/pr-18/"
      tags.push({ layer: splits[0]!, tag: `${splits[1]}/${splits[2]}` });
    } else {
      throw new Error(error);
    }
  }
  return tags;
}

/**
 * Standardize the mapsheet code to remove / and , in the paths.
 */
export function sheetCodeToPath(sheetCode: string): string {
  return sheetCode.replace(/[/,]/g, '');
}

/**
 * Override the source data link with provided data tags return.
 *
 * @param sources the original source data links from the project stac file
 * @param tags the data tags to override the source links, for example [{ layer: "airport", tag: "pull_request/pr-18/" }]
 * @param catalogUrl the catalog url to look for the source layer with tag
 *
 * @returns the override source links with the data tag applied
 *
 */
export async function overrideSource(sources: URL[], tags: dataTag[], catalogUrl: URL): Promise<URL[]> {
  for (const source of sources) {
    for (const tag of tags) {
      if (source.href.includes(tag.layer)) {
        logger.info({ source: source.href, layer: tag.layer, tag: tag.tag }, 'ProduceCover: DataOverride');
        // Find the source layer with the tag from the catalog and override the source link
        const layerCollection = await getDataFromCatalog(catalogUrl, tag.layer);
        source.href = layerCollection.href;
      }
    }
  }
  return sources;
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
  source: option({
    type: optional(Url),
    long: 'source',
    description: 'Source data catalog.json that contains the layers.',
  }),
  dataTags: option({
    type: optional(string),
    long: 'data-tags',
    description:
      'Override data tags in a string array to use when looking for source layers, for example airport/pull_request/pr-18/,contours/pull_request/pr-18/',
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
    description: 'Path or s3 bucket of the output directory to write generated map sheets.',
  }),
  strategy: option({
    long: 'strategy',
    type: StorageStrategyOption,
    description: 'Storage strategies to use, for example --strategy=latest',
  }),
  tempLocation,
};

export const ProduceCoverCommand = command({
  name: 'produce-cover',
  description: 'Read a Qgis project and mapsheet data, then generate stac files for the exports.',
  args: ProduceArgs,
  async handler(args) {
    registerFileSystem();
    const rootCatalog = new URL('catalog.json', args.output);
    logger.info({ project: args.project.href }, 'ProduceCover: Start');

    const q = qFromArgs(args);

    const mapSheets = args.fromFile != null ? args.mapSheet.concat(await fromFile(args.fromFile)) : args.mapSheet;

    // Download mapshseet layer data from the project stac file
    const stac = await fsa.readJson<StacItem>(args.project);
    if (stac == null) throw new Error(`Invalid STAC Item at path: ${args.project.href}`);

    // Download project file from the project stac file
    logger.info({ project: args.project.href }, 'DownloadProject: Start');
    let projectPath;
    for (const [key, asset] of Object.entries(stac.assets)) {
      const downloadedPath = await downloadFile(new URL(asset.href, args.project), args.tempLocation);
      if (key === 'project') projectPath = downloadedPath;
    }
    if (projectPath == null) {
      throw new Error(`Project asset not found in STAC Item: ${args.project.href}`);
    }
    logger.info({ project: args.project.href }, 'DownloadProject: End');

    logger.info({ project: args.project.href }, 'ProduceCover: PrepareSources');
    const sources: URL[] = stac.links
      .filter((link) => link.rel === 'dataset')
      .map((link) => new URL(link.href, args.project));

    // Override data with dataTag if provided
    if (args.source && args.dataTags) {
      throw new Error('--data-tags not supported');
    }

    // Download mapsheet layer to parse geometry and metadata for the export
    logger.info({ project: args.project.href, mapSheetLayer: args.mapSheetLayer }, 'DownloadMapSheet: Start');
    for (const source of sources) {
      if (source.href.includes(args.mapSheetLayer)) await downloadAssets(source, args.tempLocation);
    }
    logger.info({ project: args.project.href }, 'DownloadMapSheet: End');

    // Run python list all the mapsheet covering metadata
    const exportOptions: ExportOptions = {
      layout: args.layout,
      mapSheetLayer: args.mapSheetLayer,
      dpi: args.dpi,
      format: args.format,
    };
    logger.info({ project: args.project.href, exportOptions: exportOptions }, 'ProduceCover: ExportCover');
    const metadatas = await pyRunner.qgisExportCover(projectPath, exportOptions, args.all ? undefined : mapSheets);

    // Create Stac Files and upload to destination
    const projectName = basename(args.project.href, '.json');
    const sw = new StacCollectionWriter('product', projectName);
    sw.collection.title = `Topographic System projects ${projectName} exports ${args.format}.`;
    sw.collection.description = `LINZ Topographic QGIS Project Series ${projectName} exported maps in ${args.format} format.`;
    sw.strategy(args.strategy);

    logger.info({ project: args.project.href, number: metadatas.length }, 'ProduceCover: CreateStacItems');

    for (const metadata of metadatas) {
      const standardizedSheetCode = sheetCodeToPath(metadata.sheetCode);

      const item = sw.item(standardizedSheetCode);
      item.geometry = metadata.geometry;
      item.bbox = metadata.bbox;
      item.properties['proj:epsg'] = metadata.epsg;
      item.properties['linz:mapsheet'] = metadata.sheetCode;
      item.properties['linz_topographic_system:options'] = exportOptions;

      // Add project link
      const canonicalLink = stac.links.find((link) => link.rel === 'canonical');
      item.links.push({
        rel: 'project',
        href: canonicalLink ? canonicalLink.href : args.project.href,
        type: 'application/json',
      });

      // Add source data links
      for (const file of sources) {
        item.links.push({
          rel: 'source',
          href: file.href,
          type: 'application/json',
        });
      }

      // Add assets link if available
      item.links.push(...stac.links.filter((link) => link.rel === 'assets'));
    }

    const itemTarget = new URL(`./${projectName}.json`, args.output);
    logger.info({ destination: itemTarget.href }, 'ProduceCover: WriteStacItem');
    const collections = await sw.writeWithStrategy(itemTarget, q, true);

    if (collections.length !== 1) {
      throw new Error(`ProduceCover: Wrong number of collections for project ${args.project.href}`);
    }
    const collectionUrl = collections[0]!;

    logger.info({ project: args.project.href }, 'ProduceCover: UpsertStacCatalog');
    await StacUpdater.collections(rootCatalog, [...collections.values()], true);

    logger.info({ project: args.project.href, target: args.output.href }, 'ProduceCover: Finished');

    // If running in argo dump out output information to be used by further steps
    if (isArgo()) {
      // Prepare the item paths for group step in Argo
      const collection = await fsa.readJson<StacCollection>(collectionUrl);
      if (collection == null) throw new Error(`Invalid STAC Collection generated for project ${args.project.href}`);
      const itemsLinks = collection.links.filter((link) => link.rel === 'item');
      const items = itemsLinks.map((link) => ({ path: new URL(link.href, collectionUrl).href }));

      // Where the JSON files were written to
      await fsa.write(fsa.toUrl('/tmp/produce/cover-items.json'), JSON.stringify(items));
    }
  },
});
