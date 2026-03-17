import { tmpdir } from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import { logger, UrlFolder, stringToUrlFolder, Url, canCommentOnPr } from '@linzjs/topographic-system-shared';
import { boolean, command, flag, number, option, optional, positional, string } from 'cmd-ts';

import { CloneCommand } from './action.clone.ts';
import { DiffCommand } from './action.diff.ts';
import { ExportCommand } from './action.export.ts';
import { CommentCommand } from './action.pr.comment.ts';
import { ParquetCommand } from './action.to.parquet.ts';
import { ValidateCommand } from './action.validate.ts';
import { VersionCommand } from './action.version.ts';

const baseOutputLocation = path.join(tmpdir(), 'kart');

export const FlowCommand = command({
  name: 'kart-flow',
  description:
    'Run all kart data-review steps in order: version → clone → diff → pr-comment → export → to-parquet → validate',
  args: {
    repository: positional({
      displayName: 'repository',
      description: 'Repository to clone',
    }),
    ref: option({
      type: optional(string),
      long: 'ref',
      description: 'Branch ref to checkout (default: master)',
      defaultValue: () => 'master',
    }),
    output: option({
      type: UrlFolder,
      long: 'output',
      description: 'Location of root catalog for parquet and validation output',
    }),
    changedDatasetsOnly: flag({
      type: boolean,
      long: 'changed-datasets-only',
      description: 'Whether to only export changed datasets (default: false)',
      onMissing: () => false,
    }),

    // Clone args
    cloneOutput: option({
      type: UrlFolder,
      long: 'clone-output',
      description: 'Output directory for the cloned repository (default: repo)',
      defaultValue: () => stringToUrlFolder('repo'),
    }),

    // Diff args
    diffOutput: option({
      type: UrlFolder,
      long: 'diff-output',
      description: 'Output directory for diff results (default: $TMPDIR/kart/diff)',
      defaultValue: () => stringToUrlFolder(path.join(baseOutputLocation, 'diff')),
    }),
    summaryFile: option({
      type: Url,
      long: 'summary-file',
      description: 'Output file for summary markdown (default: pr_summary.md)',
      defaultValue: () => pathToFileURL('pr_summary.md'),
    }),

    // Export args
    exportOutput: option({
      type: UrlFolder,
      long: 'export-output',
      description: 'Output directory for exported GPKG files (default: $TMPDIR/kart/export)',
      defaultValue: () => stringToUrlFolder(path.join(baseOutputLocation, 'export')),
    }),
    exportRef: option({
      type: optional(string),
      long: 'export-ref',
      description: 'Ref to export',
      defaultValue: () => 'FETCH_HEAD',
      defaultValueIsSerializable: true,
    }),

    // To-parquet args
    compression: option({
      type: optional(string),
      long: 'compression',
      description: 'Parquet compression type',
      defaultValue: () => 'zstd',
      defaultValueIsSerializable: true,
    }),
    compressionLevel: option({
      type: optional(number),
      long: 'compression-level',
      description: 'Parquet compression level',
      defaultValue: () => 17,
      defaultValueIsSerializable: true,
    }),
    sortByBbox: flag({
      type: boolean,
      long: 'sort-by-bbox',
      description: 'Sort parquet files by bounding box (default: true)',
      onMissing: () => true,
    }),
    rowGroupSize: option({
      type: optional(number),
      long: 'row-group-size',
      description: 'Parquet row group size',
      defaultValue: () => 4096,
      defaultValueIsSerializable: true,
    }),
    parquetTempLocation: option({
      type: UrlFolder,
      long: 'parquet-temp-location',
      description: 'Temp directory for intermediate parquet files (default: $TMPDIR/kart/parquet)',
      defaultValue: () => stringToUrlFolder(path.join(baseOutputLocation, 'parquet')),
    }),

    // Validate args
    validationMode: option({
      type: string,
      long: 'validation-mode',
      description: 'Validation mode: generic or postgis (default: generic)',
      defaultValue: () => 'generic',
      defaultValueIsSerializable: true,
    }),
    configFile: option({
      type: Url,
      long: 'config-file',
      description:
        'Path to validation configuration JSON file (default: /packages/validation/config/default_config.json)',
      defaultValue: () => pathToFileURL('/packages/validation/config/default_config.json'),
    }),
    validationOutputDir: option({
      type: UrlFolder,
      long: 'validation-output-dir',
      description: 'Output directory for validation results (default: $TMPDIR/kart/validation-output)',
      defaultValue: () => stringToUrlFolder(path.join(baseOutputLocation, 'validation-output')),
    }),
    areaCrs: option({
      type: number,
      long: 'area-crs',
      description: 'CRS code for area calculations (default: 2193)',
      defaultValue: () => 2193,
      defaultValueIsSerializable: true,
    }),
    exportParquet: flag({
      type: boolean,
      long: 'export-parquet',
      description: 'Export validation results to Parquet format',
      onMissing: () => false,
    }),
    exportParquetByGeometry: flag({
      type: boolean,
      long: 'export-parquet-by-geometry',
      description: 'Export Parquet files separated by geometry type',
      onMissing: () => false,
    }),
    noExportGpkg: flag({
      type: boolean,
      long: 'no-export-gpkg',
      description: 'Disable GeoPackage export',
      onMissing: () => false,
    }),
    useDateFolder: flag({
      type: boolean,
      long: 'use-date-folder',
      description: 'Create date-based subfolder in output directory',
      onMissing: () => false,
    }),
    reportOnly: flag({
      type: boolean,
      long: 'report-only',
      description: "Don't export validation data - only a report is created",
      onMissing: () => false,
    }),
    skipQueries: flag({
      type: boolean,
      long: 'skip-queries',
      description: 'Skip query-based validations',
      onMissing: () => false,
    }),
    skipFeaturesOnLayer: flag({
      type: boolean,
      long: 'skip-features-on-layer',
      description: 'Skip features-on-layer validations',
      onMissing: () => false,
    }),
    skipSelfIntersections: flag({
      type: boolean,
      long: 'skip-self-intersections',
      description: 'Skip self-intersection validations',
      onMissing: () => false,
    }),
    verbose: flag({
      type: boolean,
      long: 'verbose',
      description: 'Enable verbose output',
      onMissing: () => false,
    }),
    bbox: option({
      type: optional(string),
      long: 'bbox',
      description: 'Bounding box for spatial filtering (minX minY maxX maxY or minX,minY,maxX,maxY)',
    }),
    date: option({
      type: optional(string),
      long: 'date',
      description: 'Date for filtering (YYYY-MM-DD or "today")',
    }),
    weeks: option({
      type: optional(number),
      long: 'weeks',
      description: 'Number of weeks back for date filtering',
    }),
  },
  async handler(args) {
    logger.info({ args }, 'Flow:Start');

    const parquetLocationForValidation = new URL('files.parquet', args.parquetTempLocation);

    logger.info('Flow:Step [1/7] version');
    await VersionCommand.handler({});

    logger.info('Flow:Step [2/7] clone');
    await CloneCommand.handler({ repository: args.repository, ref: args.ref, output: args.cloneOutput });

    logger.info('Flow:Step [3/7] diff');
    await DiffCommand.handler({
      context: args.cloneOutput,
      output: args.diffOutput,
      summaryFile: args.summaryFile,
      diff: [],
    });

    if (canCommentOnPr()) {
      logger.info('Flow:Step [4/7] pr-comment');
      await CommentCommand.handler({ pr: undefined, repo: undefined, bodyFile: args.summaryFile });
    } else {
      logger.info('Flow:Step [4/7] pr-comment (skipped - no PR detected)');
    }

    logger.info('Flow:Step [5/7] export');
    await ExportCommand.handler({
      context: args.cloneOutput,
      output: args.exportOutput,
      ref: args.exportRef,
      changed: args.changedDatasetsOnly,
      datasets: [],
    });

    logger.info('Flow:Step [6/7] to-parquet');
    await ParquetCommand.handler({
      compression: args.compression,
      compressionLevel: args.compressionLevel,
      sortByBbox: args.sortByBbox,
      rowGroupSize: args.rowGroupSize,
      output: args.output,
      tempLocation: args.parquetTempLocation,
      sourceFiles: [args.exportOutput],
    });

    logger.info('Flow:Step [7/7] validate');
    await ValidateCommand.handler({
      output: args.output,
      mode: args.validationMode,
      'db-path': parquetLocationForValidation,
      'config-file': args.configFile,
      'output-dir': args.validationOutputDir,
      'area-crs': args.areaCrs,
      'export-parquet': args.exportParquet,
      'export-parquet-by-geometry': args.exportParquetByGeometry,
      'no-export-gpkg': args.noExportGpkg,
      'use-date-folder': args.useDateFolder,
      'report-only': args.reportOnly,
      'skip-queries': args.skipQueries,
      'skip-features-on-layer': args.skipFeaturesOnLayer,
      'skip-self-intersections': args.skipSelfIntersections,
      verbose: args.verbose,
      bbox: args.bbox,
      date: args.date,
      weeks: args.weeks,
    });

    logger.info('Flow:Completed');
  },
});
