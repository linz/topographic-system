import path from 'path';
import { fileURLToPath, pathToFileURL } from 'url';

import { fsa } from '@chunkd/fs';
import { Command } from '@linzjs/docker-command';
import { logger } from '@linzjs/topographic-system-shared';
import type { GeoJSONMultiPolygon, GeoJSONPolygon } from 'stac-ts/src/types/geojson.ts';

import type { ExportOptions } from './stac.ts';

export const BaseCommandOptions = {
  useDocker: false,
  container: 'ghcr.io/linz/qgis-flatpak:linz-qgis_c03dd3-5052c5_build-33',
};

interface SheetMetadataStdOut {
  sheetCode: string;
  geometry: string; // Geometry encoded as string
  epsg: number;
  bbox: [number, number, number, number];
}

export type SheetMetadata = {
  sheetCode: string;
  geometry: GeoJSONPolygon | GeoJSONMultiPolygon;
  epsg: number;
  bbox: [number, number, number, number];
};

async function findQgisSource(): Promise<URL> {
  const sameFolder = new URL('qgis/src/qgis_export.py', import.meta.url);
  const isSameFolder = await fsa.exists(sameFolder);
  if (isSameFolder === true) return new URL('.', sameFolder);

  const parentLocation = new URL('../../qgis/src/qgis_export.py', import.meta.url);
  const isParentLocation = await fsa.exists(parentLocation);
  if (isParentLocation === true) return new URL('.', parentLocation);

  throw new Error('Unable to find QGIS source files');
}

function parseSheetsMetadata(stdoutBuffer: string): SheetMetadata[] {
  const raw = JSON.parse(stdoutBuffer) as SheetMetadataStdOut[];

  const metadata: SheetMetadata[] = [];
  for (const item of raw) {
    // FIXME: Missing some floating number like 0.25 and 0.5 and adding some floating number like 0.000000001 in the output of qgis_export_cover.py,
    // which cause the bbox to be different from the original one in qgis project and cause the stac item to be different from the original one. Need to investigate why this happens and how to fix it.
    const geom = JSON.parse(item.geometry) as GeoJSON.Geometry;

    // Only could be a polygon or multipolygons for a mapsheet.
    if (geom.type !== 'Polygon' && geom.type !== 'MultiPolygon') {
      throw new Error(`Unexpected geometry type for ${item.sheetCode}: ${geom.type}`);
    }

    metadata.push({
      sheetCode: item.sheetCode,
      geometry: geom.type === 'Polygon' ? (geom as GeoJSONPolygon) : (geom as GeoJSONMultiPolygon),
      epsg: item.epsg,
      bbox: item.bbox,
    });
  }

  return metadata;
}

/**
 * Running python commands for qgis_export
 */
async function qgisExport(input: URL, output: URL, sheetCode: string, options: ExportOptions): Promise<URL> {
  const sourceLocation = await findQgisSource();

  const cmd = Command.create('python3', BaseCommandOptions);

  cmd.mount(fileURLToPath(sourceLocation));
  cmd.mount(fileURLToPath(new URL('.', input)));
  cmd.mount(fileURLToPath(new URL('.', output)));

  cmd.args.push(fileURLToPath(new URL('qgis_export.py', sourceLocation)));
  cmd.args.push(fileURLToPath(input));
  cmd.args.push(fileURLToPath(output));
  cmd.args.push(options.layout);
  cmd.args.push(options.mapSheetLayer);
  cmd.args.push(options.format);
  cmd.args.push(options.dpi.toFixed());
  cmd.args.push(sheetCode);

  const res = await cmd.run();
  logger.debug('qgis_export.py ' + cmd.args.join(' '));

  if (res.exitCode !== 0) {
    logger.fatal({ qgis_export: res }, 'Failure');
    throw new Error('qgis_export.py failed to run');
  }

  return pathToFileURL(res.stdout.trim());
}

/**
 * Running python commands for qgis_export_cover
 * This command is used to load map sheet layers from the input project and mapsheet data and return geometry and metadata of the mapsheets.
 * @param input URL of the input QGIS project file
 * @param options ExportOptions containing layout and mapSheetLayer information
 *
 * @param mapsheets Optional to specify which mapsheets to list. If not provided, all mapsheets in the project will be listed.
 *
 * @returns mapsheet metadata including sheetcode and geometry information for the stac files
 */
export async function qgisExportCover(
  input: URL,
  options: ExportOptions,
  mapsheets?: string[],
): Promise<SheetMetadata[]> {
  const sourceLocation = await findQgisSource();
  const cmd = Command.create('python3', BaseCommandOptions);

  cmd.mount(fileURLToPath(sourceLocation));
  cmd.mount(fileURLToPath(new URL('.', input)));

  cmd.args.push(fileURLToPath(new URL('qgis_export_cover.py', sourceLocation)));
  cmd.args.push(fileURLToPath(input));
  cmd.args.push(options.layout);
  cmd.args.push(options.mapSheetLayer);
  // list all if mapsheets is not provided, otherwise list the mapsheets passed from CLI
  if (mapsheets) {
    cmd.args.push('False');
    for (const mapsheet of mapsheets) cmd.args.push(mapsheet);
  } else {
    cmd.args.push('True');
  }
  const res = await cmd.run();
  logger.debug('qgis_export_cover.py ' + cmd.args.join(' '));

  if (res.exitCode !== 0) {
    logger.fatal({ list_map_sheets: res }, 'Failure');
    throw new Error('list_map_sheets.py failed to run');
  }

  return parseSheetsMetadata(res.stdout);
}

/**
 * Running python commands for list_source_layers
 */
async function listSourceLayers(input: URL): Promise<string[]> {
  const sourceLocation = await findQgisSource();
  const cmd = Command.create('python3', BaseCommandOptions);

  cmd.mount(fileURLToPath(sourceLocation));
  cmd.mount(fileURLToPath(new URL('.', input)));

  cmd.args.push(fileURLToPath(new URL('list_source_layers.py', sourceLocation)));
  cmd.args.push(fileURLToPath(input));
  const res = await cmd.run();
  logger.debug('list_source_layers.py ' + cmd.args.join(' '));

  if (res.exitCode !== 0) {
    logger.fatal({ list_source_layers: res }, 'Failure');
    throw new Error('list_source_layers.py failed to run');
  }

  // Get all layers names and remove duplicates
  const layerPaths = JSON.parse(res.stdout) as string[];
  const layerNames = Array.from(new Set(layerPaths.map((p) => path.basename(p, path.extname(p)))));

  return layerNames;
}

/** Redefined for testing */
export const pyRunner = { listSourceLayers, qgisExport, qgisExportCover };
