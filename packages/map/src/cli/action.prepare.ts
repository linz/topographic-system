import { basename } from 'path';

import { Projection } from '@basemaps/geo';
import { fsa } from '@chunkd/fs';
import {
  concurrency,
  isArgo,
  logger,
  parquetGeometryStats,
  qFromArgs,
  readParquet,
  readParquetMetadata,
  registerFileSystem,
  Url,
  UrlFolder,
} from '@linzjs/topographic-system-shared';
import {
  geoJsonToWgs84,
  getJsonToWgs84Bbox,
  StacCollectionWriter,
  StacDownloader,
  StacUpdater,
} from '@linzjs/topographic-system-stac';
import { command, flag, multioption, option, optional, restPositionals, string } from 'cmd-ts';
import type { GeoJSONPolygon, StacCollection, StacItem, StacLink } from 'stac-ts';

import { getQgisCartoTextLayer, getQgisMapSheetDataset, getQgisProjectMeta } from '../qgis.ts';
import { type ExportOptions } from '../stac.ts';
import { ExportCommand, fromFile } from './action.export.ts';
import { FormatMultiOption } from './export.options.ts';
import { cache, tempLocation } from './shared.args.ts';

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
  assets: multioption({
    long: 'asset',
    type: FormatMultiOption,
    description: `Assets to export as key=value spec e.g. "layout=tiff-50,dpi=600,format=tiff"`,
  }),
  mapSheetDataset: option({
    type: optional(string),
    long: 'map-sheet-dataset',
    description: 'Map sheet dataset name to use for export',
  }),
  cartoTextDataset: option({
    type: optional(string),
    long: 'carto-text-dataset',
    description: 'Carto text dataset name to use for export',
  }),
  source: option({
    type: optional(Url),
    long: 'source',
    description: 'Source data catalog.json that contains the layers.',
  }),
  strategy: option({
    type: optional(string),
    long: 'strategy',
    description:
      'Optional storage strategy to filter source data collections, e.g. "commit=abc123" or "date=2026-05-19T22-18-14.595Z".',
  }),
  catalog: option({
    type: optional(Url),
    long: 'catalog',
    description:
      'Optional catalog.json URL to use with --strategy for filtering source collections by commit or date strategy.',
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
    if (args.assets.length === 0) throw new Error('No --asset provided');
    const q = qFromArgs(args);

    const mapSheets = new Set(
      args.fromFile != null ? args.mapSheet.concat(await fromFile(args.fromFile)) : args.mapSheet,
    );

    const downloader = new StacDownloader(args.tempLocation, args.cache, q);
    if (args.strategy) {
      downloader.resolvers.unshift(StacDownloader.Resolver.strategy(args.strategy));
      logger.info({ strategy: args.strategy }, 'Prepare: Storage strategy override set');
    }

    const stac = await downloader.fetchStac<StacItem>(args.project);
    logger.info({ project: args.project.href }, 'Download: Start');

    const projectAssets = await downloader.fetchAssets(args.project, (asset) => asset.href.endsWith('.qgs'));
    const qgsProject = projectAssets.find((f) => f.source.href.endsWith('.qgs'));
    if (qgsProject == null) throw new Error(`QGIS project file not found at: ${args.project.href}`);
    logger.info({ project: args.project.href }, 'Download: End');

    logger.info({ project: args.project.href }, 'Prepare');
    const projectMeta = await getQgisProjectMeta(qgsProject.target);
    const mapSheetLayer = getQgisMapSheetDataset(projectMeta.layers, args.mapSheetDataset);
    logger.info({ project: args.project.href, mapSheetLayer: mapSheetLayer.name }, 'Prepare: MapSheetLayer');

    const cartoTextLayer = getQgisCartoTextLayer(projectMeta.layers, args.cartoTextDataset);
    logger.info({ project: args.project.href, cartoTextLayer: cartoTextLayer.name }, 'Prepare: CartoTextLayer');

    const sourceAssets = await downloader.fetchLinkedAssets(
      args.project,
      (link) => link.rel === 'dataset',
      (asset) => asset.href.endsWith(mapSheetLayer.source),
    );
    const mapSheetFile = sourceAssets[0];
    if (mapSheetFile == null) throw new Error(`MapSheet asset "${mapSheetLayer.source}" not found`);

    const mapSheetMeta = await readParquetMetadata(mapSheetFile.target);
    const mapSheetGeo = await parquetGeometryStats(mapSheetMeta);
    const mapSheetProj = Projection.get(mapSheetGeo.epsg);

    // Run python list all the mapsheet covering metadata
    const exportOptions: ExportOptions = {
      mapSheetDataset: mapSheetLayer.source,
      cartoTextDataset: cartoTextLayer.source,
      assets: args.assets,
    };

    const mapSheetsToCreate: SheetMetadata[] = [];

    for await (const row of readParquet<TopoMapSheetParquet>(mapSheetFile.target, { decodeGeometry: true })) {
      if (args.all || mapSheets.has(row.sheet_code)) {
        mapSheetsToCreate.push({
          sheetCode: row.sheet_code,
          geometry: geoJsonToWgs84(row.geometry, mapSheetProj),
        });
      }
    }

    // Create Stac Files and upload to destination
    const projectName = basename(args.project.href, '.json');
    const sw = new StacCollectionWriter('product', projectName);
    const formatsStr = exportOptions.assets.map((o) => o.label ?? o.format).join(', ');
    sw.collection.title = `Topographic System projects ${projectName} exports ${formatsStr}.`;
    sw.collection.description = `LINZ Topographic QGIS Project Series ${projectName} exported maps in ${formatsStr} format.`;

    logger.info({ project: args.project.href, number: mapSheetsToCreate.length }, 'Prepare: CreateStacItems');

    const sources = await Promise.all(
      stac.asset.links
        .filter((link) => link.rel === 'dataset')
        .map(async (link) => {
          const linkUrl = new URL(link.href, args.project);
          const item = await downloader.fetchStac<StacCollection | StacItem>(linkUrl);
          if (item == null) throw new Error('Unable to find source stac for url: ' + linkUrl.href);
          return item;
        }),
    );

    for (const metadata of mapSheetsToCreate) {
      const standardizedSheetCode = sheetCodeToPath(metadata.sheetCode);

      const item = sw.item(standardizedSheetCode);
      item.geometry = metadata.geometry;
      item.bbox = getJsonToWgs84Bbox(metadata.geometry);
      item.properties['proj:epsg'] = projectMeta.epsg.code;
      item.properties['linz:mapsheet'] = metadata.sheetCode;
      item.properties['linz_topographic_system:options'] = exportOptions;

      // Add project link
      item.links.push({
        rel: 'project',
        href: stac.url.href,
        type: 'application/json',
      });

      for (const s of sources) {
        const itemLink: StacLink = {
          rel: 'source',
          href: s.url.href,
          type: 'application/json',
          // TODO: if these are canonical links, we should add file:size and file:checksum
        };

        if (typeof s.asset.title === 'string') itemLink.title = s.asset.title;
        item.links.push(itemLink);
      }

      // Add assets link if available
      // TODO do we have asset links??
      // item.links.push(...stac.links.filter((link) => link.rel === 'assets'));
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
