import { mkdir } from 'node:fs/promises';

import { fsa } from '@chunkd/fs';
import {
  Downloader,
  DownloadRels,
  logger,
  qFromArgs,
  qMapAll,
  registerFileSystem,
  Url,
  UrlArrayJsonFile,
  worker,
} from '@linzjs/topographic-system-shared';
import { HashWriter, StacUpdater } from '@linzjs/topographic-system-stac';
import { command, flag, option, optional, restPositionals } from 'cmd-ts';
import type { StacAsset, StacItem } from 'stac-ts';

import { pyRunner } from '../python.runner.ts';
import type { ExportOptions } from '../stac.ts';
import { validateTiff } from '../validate.ts';
import { type ExportFormat, ExportFormats } from './action.produce.cover.ts';
import { tempLocation } from './shared.args.ts';

function getExtentFormat(format: ExportFormat): string {
  if (format === 'pdf') return 'pdf';
  else if (format === 'tiff' || format === 'geotiff') return 'tiff';
  else if (format === 'png') return 'png';
  else throw new Error(`Invalid format`);
}

/** Ready the json file and parse all the mapsheet code as array */
export async function fromFile(file: URL): Promise<string[]> {
  const mapSheets = await fsa.readJson<string[]>(file);
  if (mapSheets == null || mapSheets.length === 0) {
    throw new Error(`Invalide or empty map sheets in file: ${file.href}`);
  }
  return mapSheets;
}

export function getContentType(format: ExportFormat): string {
  if (format === ExportFormats.Pdf) return 'application/pdf';
  else if (format === ExportFormats.Tiff) return 'image/tiff;';
  else if (format === ExportFormats.GeoTiff) return 'image/tiff; application=geotiff; profile=cloud-optimized';
  else if (format === ExportFormats.Png) return 'image/png';
  else throw new Error(`Invalid format`);
}

export const ProduceArgs = {
  worker,
  path: restPositionals({ type: Url, displayName: 'path', description: 'Paths to stac items files' }),
  fromFile: option({
    type: optional(UrlArrayJsonFile),
    long: 'from-file',
    description:
      'Path to JSON file containing array of paths to items configurations. ' +
      'File must be an array of objects with key "path" and value of a path to an item configuration.',
  }),
  tempLocation,
  force: flag({ long: 'force', description: 'Overwrite existing exported files' }),
};

export const ProduceCommand = command({
  name: 'produce',
  description: 'Produce',
  args: ProduceArgs,
  async handler(args) {
    registerFileSystem();

    const q = qFromArgs(args);

    const paths = args.fromFile != null ? args.path.concat(args.fromFile) : args.path;
    if (paths.length === 0) {
      throw new Error('At least one path to a stac item or item configuration must be provided');
    }

    const downloader = new Downloader(args.tempLocation, q);

    // Find all the assets to download from a stac item.
    for (const p of paths) {
      const stac = await fsa.readJson<StacItem>(p);
      if (stac == null) throw new Error(`Invalid STAC Item at path: ${p.href}`);

      // Add links from download rels for downloading
      stac.links
        .filter((link) => DownloadRels.has(link.rel))
        .forEach(async (link) => downloader.addStac(new URL(link.href, p), await fsa.readJson(new URL(link.href, p))));

      // Add assets for downloading
      Object.values(stac.assets ?? {}).forEach((asset) => downloader.addAsset(new URL(asset.href, p)));
    }

    // Download all the assets, including the project file and source data for the project.
    await downloader.getAllAssets();

    await qMapAll(q, paths, (p) => produce(p, downloader, args));
    await StacUpdater.items(paths, q, true);

    logger.info('Produce: Done');
  },
});

async function produce(path: URL, downloader: Downloader, args: { force: boolean; tempLocation: URL }) {
  logger.info({ path: path.href }, 'Produce: Started');
  // Prepare tmp path for the outputs
  const tempOutput = new URL('output/', args.tempLocation);
  if (tempOutput.protocol === 'file:') await mkdir(tempOutput, { recursive: true });

  // Run python qgis export script
  const stac = await fsa.readJson<StacItem>(path);
  const exportOptions = stac.properties['linz_topographic_system:options'] as ExportOptions;
  const mapSheets = stac.properties['linz:mapsheet'] as string;

  const destPath = new URL(path.href.replace('.json', `.${getExtentFormat(exportOptions.format)}`));
  if (args.force !== true && (await fsa.exists(destPath))) {
    logger.info({ destPath: destPath.href }, 'Produce:Exists, skipping');
    return;
  }

  // Download project file, assets, and source data from the project stac file
  const projectSource = stac.links.find((l) => l.rel === 'project');
  if (projectSource == null) throw new Error(`STAC Item ${path.href} does not have a project link`);
  const projectPath = await downloader.getAsset(new URL(projectSource.href, path));

  // Start to export file
  const file = await pyRunner.qgisExport(projectPath, tempOutput, mapSheets, exportOptions);

  if (exportOptions.format === ExportFormats.GeoTiff || exportOptions.format === ExportFormats.Tiff) {
    // TODO optimize tiff to COG / lossless webp
    await validateTiff(file, Number(stac.properties['proj:epsg']));
  }

  logger.info({ file: file.href }, 'Produce: FileExported');

  const asset = await HashWriter.write(destPath, file, { contentType: getContentType(exportOptions.format) });
  logger.info({ destPath: destPath.href }, 'Produce: FileUploaded');

  // StacUpdater in stac-push command will update all the collection links checksum.
  await StacUpdater.readWriteJson<StacItem>(path, (stac) => {
    if (stac == null) throw new Error(`Failed to read: ${path.href}`);
    stac.assets ??= {};

    if (stac.assets[exportOptions.format]) throw new Error('Asset already exists');

    const date = new Date().toISOString();
    stac.assets[exportOptions.format] = {
      href: `./${destPath.pathname.split('/').pop()}`,
      type: getContentType(exportOptions.format),
      roles: ['data'],
      updated: date,
      created: date,
      ...asset,
    } as StacAsset;

    return stac;
  });

  logger.info({ destPath: destPath.href }, 'Produce: StacUpdated');
}
