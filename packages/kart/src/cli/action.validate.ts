import { fsa } from '@chunkd/fs';
import { logger } from '@topographic-system/shared/src/log.ts';
import { boolean, command, flag, number, option, optional, string } from 'cmd-ts';
import { $ } from 'zx';

import { determineAssetLocation, recursiveFileSearch } from '../util.ts';

/* eslint-disable @typescript-eslint/explicit-function-return-type */
const boolFlag = (long: string, description: string) =>
  flag({ type: boolean, long, description, onMissing: () => false });
const strOption = (long: string, description: string, defaultValue?: string) =>
  option({
    type: optional(string),
    long,
    description,
    ...(defaultValue !== undefined && { defaultValue: () => defaultValue }),
  });
/* eslint-enable @typescript-eslint/explicit-function-return-type */

export const validateCommand = command({
  name: 'validate',
  description: 'Run topographic data validation',
  args: {
    mode: option({
      type: string,
      long: 'mode',
      description: 'Validation mode: generic for GPKG/Parquet, postgis for PostgreSQL/PostGIS',
      defaultValue: () => 'generic',
    }),
    'db-path': strOption(
      'db-path',
      'File path (GPKG/Parquet), Database URL (PostgreSQL connection string)',
      '/tmp/kart/parquet/files.parquet',
    ),
    'config-file': strOption(
      'config-file',
      'Path to validation configuration JSON file',
      '/packages/validation/config/default_config.json',
    ),
    'output-dir': strOption('output-dir', 'Output directory for validation results', '/tmp/validation-output'),
    'area-crs': option({
      type: number,
      long: 'area-crs',
      description: 'CRS code for area calculations (default: 2193)',
      defaultValue: () => 2193,
    }),
    'export-parquet': boolFlag('export-parquet', 'Export results to Parquet format'),
    'export-parquet-by-geometry': boolFlag(
      'export-parquet-by-geometry',
      'Export Parquet files separated by geometry type',
    ),
    'no-export-gpkg': boolFlag('no-export-gpkg', 'Disable GeoPackage export'),
    'use-date-folder': boolFlag('use-date-folder', 'Create date-based subfolder in output directory'),
    'report-only': boolFlag('report-only', "Don't export validation data - only a report is created"),
    'skip-queries': boolFlag('skip-queries', 'Skip query-based validations'),
    'skip-features-on-layer': boolFlag('skip-features-on-layer', 'Skip features-on-layer validations'),
    'skip-self-intersections': boolFlag('skip-self-intersections', 'Skip self-intersection validations'),
    bbox: strOption('bbox', 'Bounding box for spatial filtering (minX minY maxX maxY or minX,minY,maxX,maxY)'),
    date: strOption('date', 'Date for filtering (YYYY-MM-DD or "today")'),
    weeks: option({ type: optional(number), long: 'weeks', description: 'Number of weeks back for date filtering' }),
    verbose: boolFlag('verbose', 'Enable verbose output'),
  },
  async handler(args) {
    logger.info({ args }, 'ValidateCommand:Start');
    const cmdArgs: string[] = [];

    // Options with values
    const valueKeys = ['db-path', 'output-dir', 'mode', 'config-file', 'area-crs', 'date', 'weeks'] as const;
    for (const key of valueKeys) {
      if (args[key] != null) cmdArgs.push(`--${key}`, String(args[key]));
    }

    // Bounding box special case (4 values: minx miny maxx maxy from string)
    const bbox = args?.bbox?.replaceAll(' ', ',').split(',').filter(Boolean) ?? [];
    if (bbox.length === 4) {
      cmdArgs.push('--bbox', ...bbox);
    } else if (bbox.length > 0) {
      logger.error({ bbox }, 'ValidateCommand:InvalidBbox');
      throw new Error('bbox requires exactly 4 values: minX minY maxX maxY');
    }

    // Boolean flags
    const boolKeys = [
      'export-parquet',
      'export-parquet-by-geometry',
      'no-export-gpkg',
      'use-date-folder',
      'report-only',
      'skip-queries',
      'skip-features-on-layer',
      'skip-self-intersections',
      'verbose',
    ] as const;
    for (const key of boolKeys) {
      if (args[key]) cmdArgs.push(`--${key}`);
    }

    logger.info({ command: cmdArgs.join(' ') }, 'ValidateCommand:ArgumentsPrepared');
    const validationOut = await $`uv --directory /packages/validation/ run topographic_validation ${cmdArgs}`;
    if (args['output-dir']) {
      const filesToProcess = await recursiveFileSearch(fsa.toUrl(args['output-dir']));
      await Promise.all(
        filesToProcess.map(async (file) => {
          const target = determineAssetLocation(
            'data-validation',
            'validation-results',
            file.pathname.replace(args['output-dir'] ?? '', ''),
          );
          logger.info({ file: file.pathname, target: target.href }, 'ValidateCommand:UploadingResultFile');
          return fsa.write(target, fsa.readStream(file));
        }),
      );
    }
    logger.info({ validationOut: validationOut.stdout }, 'ValidateCommand:Completed');
  },
});
