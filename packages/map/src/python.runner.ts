import { Command } from '@linzjs/docker-command';
import type { GeoJSONMultiPolygon, GeoJSONPolygon } from 'stac-ts/src/types/geojson.ts';

import { logger } from '../../shared/src/log.ts';
import { toRelative } from '../../shared/src/url.ts';
import type { ExportOptions } from './cli/action.produce.ts';

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

export type ProjectGeometry = {
  geometry: GeoJSONPolygon | GeoJSONMultiPolygon;
  bbox: [number, number, number, number];
};

function parseSheetsMetadata(stdoutBuffer: string): SheetMetadata[] {
  const raw = JSON.parse(stdoutBuffer) as SheetMetadataStdOut[];

  const metadata: SheetMetadata[] = [];
  for (const item of raw) {
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
export async function qgisExport(
  input: URL,
  output: URL,
  mapsheets: string[],
  options: ExportOptions,
): Promise<SheetMetadata[]> {
  const cmd = Command.create('python3');

  cmd.args.push('qgis/src/qgis_export.py');
  cmd.args.push(toRelative(input));
  cmd.args.push(toRelative(output));
  cmd.args.push(options.format);
  cmd.args.push(options.dpi.toFixed());
  for (const mapsheet of mapsheets) cmd.args.push(mapsheet);

  const res = await cmd.run();
  logger.debug('qgis_export.py ' + cmd.args.join(' '));

  if (res.exitCode !== 0) {
    logger.fatal({ qgis_export: res }, 'Failure');
    throw new Error('qgis_export.py failed to run');
  }

  return parseSheetsMetadata(res.stdout);
}

/**
 * Running python commands for list_map_sheets
 */
export async function listMapSheets(input: URL, layerName: string = 'nz_topo_map_sheet'): Promise<string[]> {
  const cmd = Command.create('python3');

  cmd.args.push('qgis/src/list_map_sheets.py');
  cmd.args.push(toRelative(input));
  cmd.args.push(layerName);
  const res = await cmd.run();
  logger.debug('list_map_sheets.py ' + cmd.args.join(' '));

  if (res.exitCode !== 0) {
    logger.fatal({ list_map_sheets: res }, 'Failure');
    throw new Error('list_map_sheets.py failed to run');
  }

  return JSON.parse(res.stdout) as string[];
}
