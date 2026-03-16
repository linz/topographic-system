import { tmpdir } from 'node:os';
import path from 'node:path';

import { fsa } from '@chunkd/fs';
import { logger, UrlFolder, stringToUrlFolder } from '@linzjs/topographic-system-shared';
import { boolean, command, flag, option, optional, positional, string } from 'cmd-ts';

import { CloneCommand } from './action.clone.ts';
import { DiffCommand } from './action.diff.ts';
import { ExportCommand } from './action.export.ts';
import { CommentCommand } from './action.pr.comment.ts';
import { ParquetCommand } from './action.to.parquet.ts';
import { ValidateCommand } from './action.validate.ts';
import { VersionCommand } from './action.version.ts';

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
      description: 'Whether to only export changed datasets (default: true)',
      onMissing: () => false,
    }),
  },
  async handler(args) {
    logger.info({ args }, 'Flow:Start');
    const baseOutputLocation = path.join(tmpdir(), 'kart');
    const cloneOutputLocation = stringToUrlFolder('repo');
    const diffOutputLocation = stringToUrlFolder(path.join(baseOutputLocation, 'diff'));
    const summaryFileLocation = fsa.toUrl('pr_summary.md');
    const exportOutputLocation = stringToUrlFolder(path.join(baseOutputLocation, 'export'));
    const parquetOutputLocation = stringToUrlFolder(path.join(baseOutputLocation, 'parquet'));
    const parquetLocationForValidation = new URL('files.parquet', parquetOutputLocation);
    const configFileLocation = fsa.toUrl('/packages/validation/config/default_config.json');
    const validationOutputLocation = stringToUrlFolder(path.join(baseOutputLocation, 'validation-output'));

    logger.info('Flow:Step [1/7] version');
    await VersionCommand.handler({});

    logger.info('Flow:Step [2/7] clone');
    await CloneCommand.handler({ repository: args.repository, ref: args.ref, output: cloneOutputLocation });

    logger.info('Flow:Step [3/7] diff');
    await DiffCommand.handler({
      context: cloneOutputLocation,
      output: diffOutputLocation,
      summaryFile: summaryFileLocation,
      diff: [],
    });

    logger.info('Flow:Step [4/7] pr-comment');
    await CommentCommand.handler({ pr: undefined, repo: undefined, bodyFile: summaryFileLocation });

    logger.info('Flow:Step [5/7] export');
    await ExportCommand.handler({
      context: cloneOutputLocation,
      output: exportOutputLocation,
      ref: 'FETCH_HEAD',
      changed: args.changedDatasetsOnly,
      datasets: [],
    });

    logger.info('Flow:Step [6/7] to-parquet');
    await ParquetCommand.handler({
      compression: 'zstd',
      compressionLevel: 17,
      sortByBbox: true,
      rowGroupSize: 4096,
      output: args.output,
      tempLocation: parquetOutputLocation,
      sourceFiles: [exportOutputLocation],
    });

    logger.info('Flow:Step [7/7] validate');
    await ValidateCommand.handler({
      output: args.output,
      mode: 'generic',
      'db-path': parquetLocationForValidation,
      'config-file': configFileLocation,
      'output-dir': validationOutputLocation,
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

    logger.info('Flow:Completed');
  },
});
