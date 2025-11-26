import { Command } from '@linzjs/docker-command';
import { ExportFormats } from './cli/action.produce.ts';
import { toRelative } from './util.ts';
import { logger } from './log.ts';

export type ExportFormat = (typeof ExportFormats)[keyof typeof ExportFormats];

export interface ExportOptions {
  dpi: number;
  format: ExportFormat;
}

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
