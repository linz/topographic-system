import { basename } from 'path';

import { Epsg, Projection } from '@basemaps/geo';
import { fsa } from '@chunkd/fs';
import {
  concurrency,
  Downloader,
  DownloadRels,
  getDataFromCatalog,
  isArgo,
  logger,
  parquetGeometryStats,
  qFromArgs,
  readParquet,
  readParquetFileMetadata,
  registerFileSystem,
  Url,
  UrlFolder,
} from '@linzjs/topographic-system-shared';
import { geoJsonToWgs84, getJsonToWgs84Bbox, StacCollectionWriter, StacUpdater } from '@linzjs/topographic-system-stac';
import { command, flag, number, oneOf, option, optional, restPositionals, string } from 'cmd-ts';
import type { GeoJSONPolygon, StacCollection, StacItem, StacLink } from 'stac-ts';

import { getQgisMapSheetLayer, getQgisProjectMeta } from '../qgis.ts';
import { type ExportOptions } from '../stac.ts';
import { ExportCommand, fromFile } from './action.export.ts';
import { cache, tempLocation } from './shared.args.ts';

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

interface SheetMetadata {
  sheetCode: string;
  geometry: GeoJSONPolygon;
}

interface TopoMapSheetParquet {
  sheet_code: string;
  bbox: { xmin: number; ymin: number; xmax: number; ymax: number };
  geometry: GeoJSONPolygon;
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
        logger.info({ source: source.href, layer: tag.layer, tag: tag.tag }, 'Prepare: DataOverride');
        // Find the source layer with the tag from the catalog and override the source link
        const layerCollection = await getDataFromCatalog(catalogUrl, tag.layer);
        source.href = layerCollection.href;
      }
    }
  }
  return sources;
}

const ProduceArgs = {
  concurrency,
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
    type: optional(string),
    long: 'map-sheet-layer',
    description: 'Qgis Map sheet layer name to use for export',
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
  tempLocation,
  cache,
  export: flag({
    long: 'export',
    description: 'Export the assets after writing the STAC metadata',
    defaultValue: () => false,
  }),
};

export const PrepareCommand = command({
  name: 'prepare',
  description: 'Read a QGIS project and mapsheet data, then generate stac files for the exports.',
  args: ProduceArgs,
  async handler(args) {
    registerFileSystem();
    const rootCatalog = new URL('catalog.json', args.output);
    logger.info({ project: args.project.href, cache: args.cache.href }, 'Prepare: Start');

    const q = qFromArgs(args);

    const mapSheets = new Set(
      args.fromFile != null ? args.mapSheet.concat(await fromFile(args.fromFile)) : args.mapSheet,
    );

    // Download mapshseet layer data from the project stac file
    const stac = await fsa.readJson<StacItem>(args.project);
    if (stac == null) throw new Error(`Invalid STAC Item at path: ${args.project.href}`);

    // Download project file from the project stac file
    logger.info({ project: args.project.href }, 'Download: Start');
    const downloader = new Downloader(args.tempLocation, args.cache, q);
    downloader.addStac(args.project);
    downloader.addStacLinks(stac, DownloadRels, args.project);
    await downloader.getAllAssets();
    logger.info({ project: args.project.href }, 'Download: End');

    // Override data with dataTag if provided
    if (args.source && args.dataTags) {
      throw new Error('--data-tags not supported');
    }

    // Find downloaded project file
    const projectPath = downloader.findAsset((asset) => asset.url.href.endsWith('.qgs'))?.linked;
    if (projectPath == null) throw new Error(`Project file not found from downloaded assets`);

    logger.info({ project: args.project.href }, 'Prepare');
    const projectMeta = await getQgisProjectMeta(projectPath);
    const mapSheetLayer = getQgisMapSheetLayer(projectMeta.layers, args.mapSheetLayer);

    const mapSheetFile = downloader.findAsset((asset) => asset.url.href.endsWith(mapSheetLayer.source));
    if (mapSheetFile == null) throw new Error(`MapSheet asset "${mapSheetLayer.source}" not found`);

    const mapSheetMeta = await readParquetFileMetadata(mapSheetFile.linked);
    const mapSheetGeo = await parquetGeometryStats(mapSheetMeta);
    const mapSheetProj = Projection.get(mapSheetGeo.epsg);

    // Run python list all the mapsheet covering metadata
    const exportOptions: ExportOptions = {
      layout: args.layout,
      mapSheetLayerName: mapSheetLayer.name,
      dpi: args.dpi,
      format: args.format,
    };

    // FIXME mapSheetProj is 4167 but the data is in EPSG:2193
    const mapSheetsToCreate: SheetMetadata[] = [];
    for await (const row of readParquet<TopoMapSheetParquet>(mapSheetFile.linked, { decodeGeometry: true })) {
      if (args.all || mapSheets.has(row.sheet_code)) {
        mapSheetsToCreate.push({
          sheetCode: row.sheet_code,
          geometry: geoJsonToWgs84(row.geometry, Projection.get(Epsg.Nztm2000) ?? mapSheetProj),
        });
      }
    }

    // Create Stac Files and upload to destination
    const projectName = basename(args.project.href, '.json');
    const sw = new StacCollectionWriter('product', projectName);
    sw.collection.title = `Topographic System projects ${projectName} exports ${args.format}.`;
    sw.collection.description = `LINZ Topographic QGIS Project Series ${projectName} exported maps in ${args.format} format.`;

    logger.info({ project: args.project.href, number: mapSheetsToCreate.length }, 'Prepare: CreateStacItems');

    for (const metadata of mapSheetsToCreate) {
      const standardizedSheetCode = sheetCodeToPath(metadata.sheetCode);

      const item = sw.item(standardizedSheetCode);
      item.geometry = metadata.geometry;
      item.bbox = getJsonToWgs84Bbox(metadata.geometry);
      item.properties['proj:epsg'] = projectMeta.epsg.code;
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
      const sources = stac.links
        .filter((link) => link.rel === 'dataset')
        .map((link) => {
          const linkUrl = new URL(link.href, args.project);
          const item = downloader.stac.get(linkUrl.href);
          if (item == null) throw new Error('Unable to find source stac for url: ' + linkUrl.href);
          return { item, url: linkUrl };
        });

      for (const s of sources) {
        if (s.item.json == null) throw new Error(`Source stac json not found for url: ${s.url.href}`);
        const canonicalLink = s.item.json.links.find((link) => link.rel === 'canonical');

        const itemLink: StacLink = {
          rel: 'source',
          href: canonicalLink ? new URL(canonicalLink.href, s.url).href : s.url.href,
          type: 'application/json',
          // TODO: if these are canonical links, we should add file:size and file:checksum
        };

        if (typeof s.item.json.title === 'string') itemLink.title = s.item.json.title;
        item.links.push(itemLink);
      }

      // Add assets link if available
      item.links.push(...stac.links.filter((link) => link.rel === 'assets'));
    }

    const itemTarget = new URL(`./${projectName}.json`, args.output);
    logger.info({ destination: itemTarget.href }, 'Prepare: WriteStacItem');
    const collectionUrl = await sw.write(itemTarget, q);

    if (collectionUrl == null) {
      throw new Error(`Prepare: Failed to write collection for project ${args.project.href}`);
    }

    logger.info({ project: args.project.href }, 'Prepare: UpsertStacCatalog');
    await StacUpdater.collections(rootCatalog, [collectionUrl], true);

    logger.info({ project: args.project.href, target: args.output.href }, 'Prepare: Finished');

    // Prepare the item paths for group step in Argo
    const collection = await fsa.readJson<StacCollection>(collectionUrl);
    if (collection == null) throw new Error(`Invalid STAC Collection generated for project ${args.project.href}`);
    const itemsLinks = collection.links.filter((link) => link.rel === 'item');
    const items = itemsLinks.map((link) => ({ path: new URL(link.href, collectionUrl).href }));

    // If running in argo dump out output information to be used by further steps
    if (isArgo()) {
      // Where the JSON files were written to
      await fsa.write(fsa.toUrl('/tmp/produce/cover-items.json'), JSON.stringify(items));
    }

    if (args.export) {
      await ExportCommand.handler({
        path: items.map((m) => new URL(m.path)),
        cache: args.cache,
        tempLocation: args.tempLocation,
        fromFile: undefined,
        force: false,
        worker: args.concurrency,
      });
    }
  },
});
