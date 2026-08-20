import { mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

import { fsa } from '@chunkd/fs';
import { Command } from '@linzjs/docker-command';
import {
  logger,
  qFromArgs,
  qMapAll,
  registerFileSystem,
  Url,
  UrlArrayJsonFile,
  worker,
} from '@linzjs/topographic-system-shared';
import { HashWriter, StacDownloader, StacUpdater } from '@linzjs/topographic-system-stac';
import { command, flag, option, optional, restPositionals } from 'cmd-ts';
import type { StacAsset, StacItem } from 'stac-ts';

import { BaseCommandOptions, pyRunner, runAndLog } from '../python.runner.ts';
import type { ExportOptions } from '../stac.ts';
import { validator } from '../validate.ts';
import type { ExportAsset, ExportFormat } from './export.options.ts';
import { ExportFormats } from './export.options.ts';
import { cache, tempLocation } from './shared.args.ts';

export function getFormatExtension(format: ExportFormat): string {
  if (format === 'pdf') return 'pdf';
  else if (format === 'tiff' || format === 'geotiff') return 'tiff';
  else if (format === 'png') return 'png';
  else if (format === 'webp') return 'webp';
  else throw new Error(`Invalid format`);
}

export function getAssetSuffix(asset: ExportAsset): string {
  if (asset.role == null || asset.role === 'data') return getFormatExtension(asset.format);
  // For non data roles, include them in the target file, eg .thumbnail.webp
  return `${asset.role}.${getFormatExtension(asset.format)}`;
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
  else if (format === ExportFormats.Webp) return 'image/webp';
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
  cache,
};

export const DownloadRels = new Set(['source', 'derived_from', 'project']);

export const ExportCommand = command({
  name: 'export',
  description: 'Export a collection of mapsheets from a prepared',
  args: ProduceArgs,
  async handler(args) {
    registerFileSystem();

    const q = qFromArgs(args);

    const paths = args.fromFile != null ? args.path.concat(args.fromFile) : args.path;
    if (paths.length === 0) throw new Error('At least one path to a stac item or item configuration must be provided');

    const downloader = new StacDownloader(args.tempLocation, args.cache, q);

    const items = await Promise.all(
      paths
        .filter((f) => f.href.endsWith('.json'))
        .map((m) => downloader.fetchLinkedAssets(m, (link) => DownloadRels.has(link.rel))),
    );

    const qgsProject = items.flat().find((f) => f.source.href.endsWith('.qgs'));
    if (qgsProject == null) throw new Error(`Project file not found from downloaded assets`);

    await qMapAll(q, paths, (p) => produce(p, qgsProject.target, args));
    await StacUpdater.items(paths, q, true);

    logger.info('Produce: Done');
  },
});

async function produce(path: URL, projectPath: URL, args: { force: boolean; tempLocation: URL }) {
  logger.info({ path: path.href }, 'Produce: Started');
  // Prepare tmp path for the outputs
  const tempOutput = new URL('output/', args.tempLocation);
  if (tempOutput.protocol === 'file:') await mkdir(tempOutput, { recursive: true });

  // Run python qgis export script
  const stac = await fsa.readJson<StacItem>(path);
  const exportOptions = stac.properties['linz_topographic_system:options'] as ExportOptions;
  const mapSheets = stac.properties['linz:mapsheet'] as string;

  for (const exportAsset of exportOptions.assets) {
    const destPath = new URL(path.href.replace('.json', `.${getAssetSuffix(exportAsset)}`));
    if (args.force !== true && (await fsa.exists(destPath))) {
      logger.info({ destPath: destPath.href }, 'Produce:Exists, skipping');
      continue;
    }

    // Start to export file
    let file = await pyRunner.qgisExport(projectPath, tempOutput, mapSheets, exportOptions, exportAsset);

    if (exportAsset.format === ExportFormats.GeoTiff || exportAsset.format === ExportFormats.Tiff) {
      file = await optimizeTiff(file);
      // TODO optimize tiff to COG / lossless webp
      await validator.validateTiff(file, Number(stac.properties['proj:epsg']));
    }

    logger.info({ file: file.href }, 'Produce: FileExported');

    const asset = await HashWriter.write(destPath, file, { contentType: getContentType(exportAsset.format) });
    logger.info({ destPath: destPath.href }, 'Produce: FileUploaded');

    // StacUpdater in stac-push command will update all the collection links checksum.
    await StacUpdater.readWriteJson<StacItem>(path, (stac) => {
      if (stac == null) throw new Error(`Failed to read: ${path.href}`);
      stac.assets ??= {};

      if (stac.assets[exportAsset.format]) throw new Error('Asset already exists');

      const date = new Date().toISOString();
      stac.assets[exportAsset.label ?? exportAsset.format] = {
        href: `./${destPath.pathname.split('/').pop()}`,
        type: getContentType(exportAsset.format),
        roles: [exportAsset.role ?? 'data'],
        updated: date,
        created: date,
        ...asset,
      } as StacAsset;
      logger.info({ destPath: destPath.href }, 'Produce: StacUpdated');

      return stac;
    });
  }
}

const GdalTranslate = new Command('gdal_translate', BaseCommandOptions);

async function optimizeTiff(file: URL): Promise<URL> {
  if (file.protocol !== 'file:') {
    logger.warn({ path: file.href }, 'Unable to optimize remote tiffs');
    return file;
  }

  const sourcePath = fileURLToPath(file);
  const targetPath = sourcePath + '.cog.tiff';

  const cmd = GdalTranslate.create(BaseCommandOptions);

  cmd.mount(fileURLToPath(new URL('.', file)));

  cmd.args.push('-q');
  cmd.args.push('-of', 'COG');
  cmd.args.push('-stats');
  cmd.args.push('-co', 'compress=webp');
  cmd.args.push('-co', 'quality=100'); // lossless webp
  cmd.args.push('-co', 'blocksize=512');
  cmd.args.push('-co', 'num_threads=ALL_CPUS');

  cmd.args.push('-co', 'overview_quality=90'); // overviews can be lossy
  cmd.args.push('-co', 'overview_resampling=lanczos');

  cmd.args.push(sourcePath);
  cmd.args.push(targetPath);

  await runAndLog(cmd, 'GDAL', 'gdal_translate');

  return fsa.toUrl(targetPath);
}
