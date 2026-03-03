import path from 'node:path';
import { logger, toRelative } from '@linzjs/topographic-system-shared';
import type { GeoJSONMultiPolygon, GeoJSONPolygon } from 'stac-ts/src/types/geojson.ts';
import { pathToFileURL } from 'url';
import { $ } from 'zx';

import type { ExportOptions } from './stac.ts';

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
export async function qgisExport(input: URL, output: URL, sheetCode: string, options: ExportOptions): Promise<URL> {
  const command = [
    'python3 qgis/src/qgis_export.py',
    toRelative(input),
    toRelative(output),
    options.layout,
    options.mapSheetLayer,
    options.format,
    options.dpi.toFixed(),
    sheetCode,
  ];

  const res = await $`${command.join(' ')}`;
  logger.debug('qgis_export.py ' + command.join(' '));

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
  const command = ['python3 qgis/src/qgis_export_cover.py', toRelative(input), options.layout, options.mapSheetLayer];

  // list all if mapsheets is not provided, otherwise list the mapsheets passed from CLI
  if (mapsheets) {
    command.push('False');
    for (const mapsheet of mapsheets) command.push(mapsheet);
  } else {
    command.push('True');
  }

  const res = await $`${command.join(' ')}`;
  logger.debug('qgis_export_cover.py ' + command.join(' '));
  if (res.exitCode !== 0) {
    logger.fatal({ list_map_sheets: res }, 'Failure');
    throw new Error('list_map_sheets.py failed to run');
  }

  return parseSheetsMetadata(res.stdout);
}

/**
 * Running python commands for list_source_layers
 */
<<<<<<< HEAD
async function listSourceLayers(input: URL): Promise<string[]> {
  const cmd = Command.create('python3');

  cmd.args.push('qgis/src/list_source_layers.py');
  cmd.args.push(toRelative(input));
  const res = await cmd.run();
  logger.debug('list_source_layers.py ' + cmd.args.join(' '));
=======
export async function listSourceLayers(input: URL): Promise<string[]> {
  const command = ['python3 qgis/src/list_source_layers.py', toRelative(input)];
>>>>>>> 390b121 (use zx instead of docker-command)

  const res = await $`${command.join(' ')}`;
  logger.debug('list_source_layers.py ' + command.join(' '));
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
