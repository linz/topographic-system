import { fsa } from '@chunkd/fs';
import { CliId, createStacCatalog, createStacItem, createStacLink, downloadFile, downloadFromCollection, getDataFromCatalog, isArgo, logger, registerFileSystem, Url, UrlFolder } from '@linzjs/topographic-system-shared';
import { command, flag, number, oneOf, option, optional, restPositionals, string } from 'cmd-ts';
import type { StacCatalog, StacItem } from 'stac-ts';

import { qgisExportCover } from '../python.runner.ts';
import { createMapSheetStacCollection, type ExportOptions, type MapSheetStacItem } from '../stac.ts';
import { fromFile } from './action.produce.ts';

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
  return sheetCode.replace(/[\/,]/g, '');
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
        const layerCollection = await getDataFromCatalog(catalogUrl, tag.layer, tag.tag);
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
    description: 'Path or s3 of the output directory to write generated map sheets.',
  }),
};

export const produceCoverCommand = command({
  name: 'produce-cover',
  description: 'Read a Qgis project and mapsheet data, then generate stac files for the exports.',
  args: ProduceArgs,
  async handler(args) {
    registerFileSystem();
    logger.info({ project: args.project }, 'ProduceCover: Start');

    const mapSheets = args.fromFile != null ? args.mapSheet.concat(await fromFile(args.fromFile)) : args.mapSheet;

    // Download mapshseet layer data from the project stac file
    const stac = await fsa.readJson<StacItem>(args.project);
    if (stac == null) throw new Error(`Invalid STAC Item at path: ${args.project.href}`);

    // Download project file from the project stac file
    logger.info({ project: args.project.href }, 'DownloadProject: Start');
    let projectPath;
    for (const [key, asset] of Object.entries(stac.assets)) {
      const downloadedPath = await downloadFile(new URL(asset.href));
      if (key === 'project') projectPath = downloadedPath;
    }
    if (projectPath == null) {
      throw new Error(`Project asset not found in STAC Item: ${args.project.href}`);
    }
    logger.info({ project: args.project.href }, 'DownloadProject: End');

    logger.info({ project: args.project.href }, 'ProduceCover: PrepareSources');
    const sources: URL[] = stac.links.filter((link) => link.rel === 'dataset').map((link) => new URL(link.href));
    // Override data with dataTag if provided
    if (args.source && args.dataTags) {
      logger.info(
        { project: args.project.href, sources: sources.length, dataTag: args.dataTags },
        'ProduceCover: OverRideSources',
      );
      const tags = parseDataTag(args.dataTags);
      await overrideSource(sources, tags, args.source);
    }

    // Download mapsheet layer to parse geometry and metadata for the export
    logger.info({ project: args.project.href, mapSheetLayer: args.mapSheetLayer }, 'DownloadMapSheet: Start');
    for (const source of sources) {
      if (source.href.includes(args.mapSheetLayer)) {
        await downloadFromCollection(source);
      }
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
    const metadatas = await qgisExportCover(projectPath, exportOptions, args.all ? undefined : mapSheets);

    // Create Stac Files and upload to destination
    logger.info({ project: args.project.href, number: metadatas.length }, 'ProduceCover: CreateStacItems');
    const items = [];
    const derivedProjectLink = stac.links.find((link) => link.rel === 'derived_from');
    const links = createStacLink(sources, derivedProjectLink ? new URL(derivedProjectLink.href) : args.project);
    for (const metadata of metadatas) {
      const standardizedSheetCode = sheetCodeToPath(metadata.sheetCode);
      const item = createStacItem(
        standardizedSheetCode,
        links,
        {},
        metadata.geometry,
        metadata.bbox,
      ) as MapSheetStacItem;
      item.properties['proj:epsg'] = metadata.epsg;
      item.properties['linz:mapsheet'] = metadata.sheetCode;
      item.properties['linz_topographic_system:options'] = exportOptions;
      // Add assets link if available
      item.links.push(...stac.links.filter((link) => link.rel === 'assets'));

      const itemPath = new URL(`/${CliId}/${standardizedSheetCode}.json`, args.output);
      items.push({ path: itemPath });
      await fsa.write(itemPath, JSON.stringify(item, null, 2));
    }

    // Create collection file
    const collectionPath = new URL(`/${CliId}/collection.json`, args.output);
    logger.info(
      { project: args.project.href, collectionPath: collectionPath.href },
      'ProduceCover: CreateStacCollection',
    );
    const collection = createMapSheetStacCollection(metadatas, links);
    await fsa.write(collectionPath, JSON.stringify(collection, null, 2));

    const catalogPath = new URL(`catalog.json`, args.output);
    logger.info({ project: args.project.href, catalogPath: catalogPath.href }, 'ProduceCover: CreateStacCatalog');
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

    // If running in argo dump out output information to be used by further steps
    if (isArgo()) {
      // Where the JSON files were written to
      await fsa.write(fsa.toUrl('/tmp/produce/cover-items.json'), JSON.stringify(items));
    }
  },
});
