import { tmpdir } from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import {
  logger,
  UrlFolder,
  stringToUrlFolder,
  Url,
  canCommentOnPr,
  concurrency,
  worker,
} from '@linzjs/topographic-system-shared';
import { boolean, command, flag, number, option, optional, positional, string } from 'cmd-ts';

import { CloneCommand } from './action.clone.ts';
import { DiffCommand } from './action.diff.ts';
import { ExportCommand } from './action.export.ts';
import { CommentCommand } from './action.pr.comment.ts';
import { ParquetCommand } from './action.to.parquet.ts';
import { ValidateCommand } from './action.validate.ts';
import { VersionCommand } from './action.version.ts';

const baseOutputLocation = path.join(tmpdir(), 'kart');

/**
 * Helper to start a new group for GitHub actions logging.
 * Previous group will close implicitly when a new groups starts.
 * If no name is provided, it will just close the current group.
 * Flushes the logger so messages appear in the correct group.
 * @param name - Name of the group to start. If not provided, it will close the current group.
 */
function ghGroupLog(name?: string) {
  if (!name) {
    process.stdout.write('::endgroup::\n');
  } else {
    process.stdout.write(`::group::${name}\n`);
    logger.info(name);
  }
}

export const FlowCommand = command({
  name: 'kart-prepare',
  description:
    'Run all kart data-review steps in order: version → clone → diff → pr-comment → export → to-parquet → validate',
  args: {
    concurrency,
    worker,
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
  },
  async handler(args) {
    logger.info({ args }, 'Flow:Start');

    const parquetLocationForValidation = new URL('files.parquet', args.parquetTempLocation);

    ghGroupLog('Flow:Step [1/7] version');
    await VersionCommand.handler({});

    ghGroupLog('Flow:Step [2/7] clone');
    await CloneCommand.handler({ repository: args.repository, ref: args.ref, output: args.cloneOutput });

    ghGroupLog('Flow:Step [3/7] diff');
    await DiffCommand.handler({
      context: args.cloneOutput,
      output: args.diffOutput,
      summaryFile: args.summaryFile,
      diff: [],
    });

    if (canCommentOnPr()) {
      ghGroupLog('Flow:Step [4/7] pr-comment');
      await CommentCommand.handler({ pr: undefined, repo: undefined, bodyFile: args.summaryFile });
    } else {
      ghGroupLog('Flow:Step [4/7] pr-comment (skipped - no PR detected)');
    }

    ghGroupLog('Flow:Step [5/7] export');
    await ExportCommand.handler({
      worker: args.worker,
      context: args.cloneOutput,
      output: args.exportOutput,
      ref: args.exportRef,
      changed: args.changedDatasetsOnly,
      datasets: [],
    });

    ghGroupLog('Flow:Step [6/7] to-parquet');
    await ParquetCommand.handler({
      worker: args.worker,
      compression: args.compression,
      compressionLevel: args.compressionLevel,
      sortByBbox: args.sortByBbox,
      rowGroupSize: args.rowGroupSize,
      output: args.output,
      tempLocation: args.parquetTempLocation,
      sourceFiles: [args.exportOutput],
    });

    ghGroupLog('Flow:Step [7/7] validate');
    await ValidateCommand.handler({
      output: args.output,
      mode: 'generic',
      'db-path': parquetLocationForValidation,
      'config-file': args.configFile,
      'output-dir': args.validationOutputDir,
      'area-crs': 2193,
      'export-parquet': false,
      'export-parquet-by-geometry': false,
      'no-export-gpkg': false,
      'use-date-folder': false,
      'report-only': false,
      'skip-queries': false,
      'skip-features-on-layer': false,
      'skip-self-intersections': false,
      verbose: false,
      bbox: undefined,
      date: undefined,
      weeks: undefined,
    });

    ghGroupLog('Flow:Completed');
    logger.info(
      'Note: If you want to run individual steps separately, you can use the corresponding commands: clone → diff → pr-comment → export → to-parquet → validate',
    );
    ghGroupLog();
  },
});
