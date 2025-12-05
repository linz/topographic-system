import { Command } from '@linzjs/docker-command';

import type { ExportOptions } from './cli/action.produce.ts';
import { logger } from './log.ts';
import { toRelative } from './util.ts';

/**
 * Running python commands for qgis_export
 */
export async function qgisExport(input: URL, output: URL, mapsheets: string[], options: ExportOptions): Promise<void> {
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
