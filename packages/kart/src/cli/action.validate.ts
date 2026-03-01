import { fsa } from '@chunkd/fs';
import {
  determineAssetLocation,
  logger,
  recursiveFileSearch,
  registerFileSystem,
  upsertAssetToItem,
} from '@linzjs/topographic-system-shared';
import { boolean, command, flag, number, option, optional, string } from 'cmd-ts';
import { $ } from 'zx';

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

/** Parse bbox string (comma or space separated) into array of 4 values */
export function parseBbox(bbox: string | undefined): string[] {
  return bbox?.replaceAll(' ', ',').split(',').filter(Boolean) ?? [];
}

/** Argument types for validation command */
export interface ValidateArgs {
  mode: string;
  'db-path': string;
  'config-file': string;
  'output-dir': string;
  'area-crs': number;
  'export-parquet': boolean;
  'export-parquet-by-geometry': boolean;
  'no-export-gpkg': boolean;
  'use-date-folder': boolean;
  'report-only': boolean;
  'skip-queries': boolean;
  'skip-features-on-layer': boolean;
  'skip-self-intersections': boolean;
  verbose: boolean;
  bbox: string | undefined;
  date: string | undefined;
  weeks: number | undefined;
}

/** Build command-line arguments for the validation CLI */
export async function buildValidationArgs(args: ValidateArgs): Promise<string[]> {
  const cmdArgs: string[] = [];

  // Check available layers and create filtered config
  const availableLayers = await getAvailableLayers(args['db-path']);
  logger.info({ layerCount: availableLayers.size, layers: [...availableLayers] }, 'ValidateCommand:AvailableLayers');

  if (availableLayers.size === 0) {
    logger.error({ dbPath: args['db-path'] }, 'ValidateCommand:NoLayersFound');
    throw new Error(`No parquet files found in: ${args['db-path']}`);
  }
  const filteredConfigPath = await createFilteredConfig(args['config-file'], availableLayers, true);

  // Use filtered config instead of original
  cmdArgs.push('--config-file', filteredConfigPath);

  // Options with values or only keys
  const valueKeys = ['db-path', 'output-dir', 'mode', 'area-crs', 'date', 'weeks'] as const;
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

  for (const key of valueKeys) {
    if (args[key] != null) cmdArgs.push(`--${key}`, String(args[key]));
  }

  // Bounding box special case (4 values: minx miny maxx maxy from string)
  const bbox = parseBbox(args.bbox);
  if (bbox.length === 4) {
    cmdArgs.push('--bbox', ...bbox);
  } else if (bbox.length > 0) {
    logger.error({ bbox }, 'ValidateCommand:InvalidBbox');
    throw new Error('bbox requires exactly 4 values: minX minY maxX maxY');
  }

  for (const key of boolKeys) {
    if (args[key]) cmdArgs.push(`--${key}`);
  }

  return cmdArgs;
}

async function getAvailableLayers(dbPath: string): Promise<Set<string>> {
  const dbLocation = fsa.toUrl(dbPath);
  const dbBasepath = new URL('./', dbLocation);
  const files = await recursiveFileSearch(dbBasepath, '.parquet');
  const layers = new Set<string>();
  for await (const file of files) {
    const fileName = file.pathname.split('/').pop() ?? '';
    layers.add(fileName.replace(/\.parquet$/, ''));
  }
  return layers;
}

interface Rule {
  table?: string;
  line_table?: string;
  intersection_table?: string;
  layername?: string;
  column?: string;
  where?: string;
  rule?: string;
  message: string;
  [key: string]: unknown;
}

export interface ValidationConfig {
  [key: string]: Rule[];
}

export async function createFilteredConfig(
  configPath: string,
  availableLayers: Set<string>,
  strict: boolean = false,
): Promise<string> {
  logger.info({ configPath, availableLayers }, 'ValidateCommand:CreatingFilteredConfig');
  const configLocation = fsa.toUrl(configPath);
  const configContent = await fsa.read(configLocation);
  const config = JSON.parse(configContent.toString()) as ValidationConfig;
  const filteredConfig: ValidationConfig = {};

  for (const [key, rules] of Object.entries(config)) {
    filteredConfig[key] = rules.filter((rule) => {
      const tables = [rule.table, rule.intersection_table, rule.line_table].filter((t): t is string => t != null);
      if (strict) {
        return tables.every((t) => availableLayers.has(t));
      } else {
        return tables.some((t) => availableLayers.has(t));
      }
    });
    logger.info(
      { key, originalCount: rules.length, filteredCount: filteredConfig[key].length },
      'ValidateCommand:RuleFiltering',
    );
  }

  const filteredConfigPath = '/tmp/filtered_config.json';
  await fsa.write(fsa.toUrl(filteredConfigPath), JSON.stringify(filteredConfig, null, 2));
  logger.info({ availableLayers: [...availableLayers], filteredConfigPath }, 'ValidateCommand:FilteredConfigCreated');
  return filteredConfigPath;
}

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
    'db-path': option({
      type: string,
      long: 'db-path',
      description: 'File path (GPKG/Parquet), Database URL (PostgreSQL connection string)',
      defaultValue: () => '/tmp/kart/parquet/files.parquet',
    }),
    'config-file': option({
      type: string,
      long: 'config-file',
      description: 'Path to validation configuration JSON file',
      defaultValue: () => '/packages/validation/config/default_config.json',
    }),
    'output-dir': option({
      type: string,
      long: 'output-dir',
      description: 'Output directory for validation results',
      defaultValue: () => '/tmp/validation-output',
    }),
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
    registerFileSystem();
    logger.info({ args }, 'ValidateCommand:Start');
    const cmdArgs = await buildValidationArgs(args);

    logger.info({ command: cmdArgs.join(' ') }, 'ValidateCommand:ArgumentsPrepared');
    const validationOut = await $`uv --directory /packages/validation/ run topographic_validation ${cmdArgs}`;
    if (args['output-dir']) {
      const filesToProcess = await recursiveFileSearch(fsa.toUrl(args['output-dir']));
      await Promise.all(
        filesToProcess.map(async (file: URL) => {
          const target = determineAssetLocation(
            'data-validation',
            'validation-results',
            file.pathname.replace(args['output-dir'] ?? '', ''),
          );
          logger.info({ file: file.pathname, target: target.href }, 'ValidateCommand:UploadingResultFile');
          await fsa.write(target, fsa.readStream(file));
          await upsertAssetToItem(target);
        }),
      );
    }
    logger.info({ validationOut: validationOut.stdout }, 'ValidateCommand:Completed');
  },
});
