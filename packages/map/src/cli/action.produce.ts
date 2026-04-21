import { mkdir } from 'node:fs/promises';

import { fsa } from '@chunkd/fs';
import { downloadProject, logger, registerFileSystem, Url, UrlArrayJsonFile } from '@linzjs/topographic-system-shared';
import { HashWriter, StacUpdater } from '@linzjs/topographic-system-stac';
import { command, flag, number, option, optional, restPositionals } from 'cmd-ts';
import pLimit from 'p-limit';
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

  concurrency: option({
    long: 'concurrency',
    description: 'Number of concurrent exports to run',
    type: number,
    defaultValue: () => 4,
    defaultValueIsSerializable: true,
  }),
};

export const ProduceCommand = command({
  name: 'produce',
  description: 'Produce',
  args: ProduceArgs,
  async handler(args) {
    registerFileSystem();

    const q = pLimit(args.concurrency);

    const paths = args.fromFile != null ? args.path.concat(args.fromFile) : args.path;
    if (paths.length === 0) {
      throw new Error('At least one path to a stac item or item configuration must be provided');
    }

    // TODO:
    // The downloader should be a lot smarter about handling concurrent downloads from
    // multiple projects, having to do this before the produce step is wrong and
    // should be fixed in the future.
    for (const p of args.path) await downloadProject(p, args.tempLocation);

    await Promise.all(args.path.map((p) => q(() => produce(p, args))));
    await StacUpdater.items(args.path, q, true);

    logger.info('Produce: Done');
  },
});

async function produce(path: URL, args: { force: boolean; tempLocation: URL }) {
  logger.info({ path: path.href }, 'Produce: Started');
  // Prepare tmp path for the outputs
  const tempOutput = new URL('output/', args.tempLocation);
  if (tempOutput.protocol === 'file:') await mkdir(tempOutput, { recursive: true });

  // Run python qgis export script
  const stac = await fsa.readJson<StacItem>(path);
  const exportOptions = stac.properties['linz_topographic_system:options'] as ExportOptions;
  const sheetCode = stac.properties['linz:mapsheet'] as string;

  const destPath = new URL(path.href.replace('.json', `.${getExtentFormat(exportOptions.format)}`));
  if (args.force !== true && (await fsa.exists(destPath))) {
    logger.info({ destPath: destPath.href }, 'Produce:Exists, skipping');
    return;
  }

  // Download project file, assets, and source data from the project stac file
  const projectPath = await downloadProject(path, args.tempLocation);

  // Start to export file
  const file = await pyRunner.qgisExport(projectPath, tempOutput, sheetCode, exportOptions);

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
