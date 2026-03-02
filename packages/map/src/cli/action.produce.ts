import { fsa } from '@chunkd/fs';
import {
  createFileStats,
  downloadProject,
  logger,
  registerFileSystem,
  tmpFolder,
  Url,
  UrlArrayJsonFile,
} from '@linzjs/topographic-system-shared';
import { command, flag, option, optional, restPositionals } from 'cmd-ts';
import { mkdirSync } from 'fs';
import type { StacAsset, StacItem } from 'stac-ts';

import { qgisExport } from '../python.runner.ts';
import type { ExportOptions } from '../stac.ts';
import { validateTiff } from '../validate.ts';
import { type ExportFormat, ExportFormats } from './action.produce.cover.ts';

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
  else if (format === ExportFormats.Tiff) return 'image/tiff';
  else if (format === ExportFormats.GeoTiff) return 'image/tiff; application=geotiff';
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
  force: flag({ long: 'force', description: 'Overwrite existing exported files' }),
};

export const ProduceCommand = command({
  name: 'produce',
  description: 'Produce',
  args: ProduceArgs,
  async handler(args) {
    registerFileSystem();

    const paths = args.fromFile != null ? args.path.concat(args.fromFile) : args.path;
    if (paths.length === 0) {
      throw new Error('At least one path to a stac item or item configuration must be provided');
    }

    // Prepare tmp path for the outputs
    const tempOutput = new URL('output/', tmpFolder);
    mkdirSync(tempOutput, { recursive: true });

    for (const path of paths) {
      logger.info({ path: path.href }, 'Produce: Started');

      // Download project file, assets, and source data from the project stac file
      const projectPath = await downloadProject(path);

      // Run python qgis export script
      const stac = await fsa.readJson<StacItem>(path);
      const exportOptions = stac.properties['linz_topographic_system:options'] as ExportOptions;
      const mapSheets = stac.properties['linz:mapsheet'] as string;

      const destPath = new URL(path.href.replace('.json', `.${getExtentFormat(exportOptions.format)}`));
      if ((await fsa.exists(destPath)) && !args.force) {
        logger.info({ destPath: destPath.href }, 'Produce: File already exists, skipping');
        continue;
      }

      // Start to export file
      const file = await qgisExport(projectPath, tempOutput, mapSheets, exportOptions);
      if (exportOptions.format === ExportFormats.GeoTiff || exportOptions.format === ExportFormats.Tiff) {
        await validateTiff(file, Number(stac.properties['proj:epsg']));
      }

      logger.info({ file: file.href }, 'Produce: FileExported');
      const stream = fsa.readStream(file);
      await fsa.write(destPath, stream, {
        contentType: getContentType(exportOptions.format),
      });
      logger.info({ destPath: destPath.href }, 'Produce: FileUploaded');

      // Add stac asset for the generated file
      const assets = {
        extent: {
          href: `./${destPath.pathname.split('/').pop()}`,
          type: getContentType(exportOptions.format),
          roles: ['data'],
          ...(await createFileStats(destPath)),
        } as StacAsset,
      };
      stac.assets = assets;
      await fsa.write(path, JSON.stringify(stac, null, 2));
      logger.info({ destPath: destPath.href }, 'Produce: StacUpdated');
    }
  },
});
