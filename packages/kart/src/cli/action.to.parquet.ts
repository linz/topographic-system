import { fsa } from '@chunkd/fs';
import { logger } from '@topographic-system/shared/src/log.ts';
import { ConcurrentQueue } from '@topographic-system/shared/src/queue.ts';
import { boolean, command, flag, number, option, optional, restPositionals, string } from 'cmd-ts';
import os from 'os';
import { basename } from 'path';
import { $ } from 'zx';

const Concurrency = os.cpus().length;
const Q = new ConcurrentQueue(Concurrency);
export const parquetCommand = command({
  name: 'to-parquet',
  description: 'Convert gpkg files in a folder to parquet format',
  args: {
    compression: option({
      type: optional(string),
      long: 'compression',
      description: 'compression type for parquet files (default: zstd)',
      defaultValue: () => 'zstd',
    }),
    compression_level: option({
      type: optional(number),
      long: 'compression-level',
      description: 'compression level for parquet files (default: 17)',
      defaultValue: () => 17,
    }),
    // Note: inverted logic due to bug/feature in cmd-ts flag defaults (flag not set always means false, regardless of defaultValue)
    no_sort_by_bbox: flag({
      type: boolean,
      defaultValue: () => false,
      long: 'no-sort-by-bbox',
      description: 'whether to _not_ sort parquet files by bounding box (default: false)',
    }),
    row_group_size: option({
      type: optional(number),
      long: 'row-group-size',
      description: 'row group size for parquet files (default: 4096)',
      defaultValue: () => 4096,
    }),
    sourceFiles: restPositionals({
      type: string,
      description: 'List of folders or files to convert (default: all .gpkg files in ./export)',
    }),
  },
  async handler(args) {
    delete $.env['GITHUB_ACTION_REPOSITORY'];
    delete $.env['GITHUB_ACTION_REF'];
    delete $.env['GITHUB_WORKFLOW_REF'];

    logger.info(
      {
        concurrency: Concurrency,
        compression: args.compression,
        compression_level: args.compression_level,
        sort_by_bbox: !args.no_sort_by_bbox,
      },
      'ToParquet:Start',
    );

    const filesToProcess: string[] = [];
    const sourceFileArguments = args.sourceFiles.length > 0 ? args.sourceFiles : ['./export'];

    for (const sourceFileArgument of sourceFileArguments) {
      const sourcePath = fsa.toUrl(sourceFileArgument);
      const stat = await fsa.head(sourcePath);
      if (stat && stat.isDirectory) {
        const filePaths = await fsa.toArray(fsa.list(sourcePath, { recursive: true }));
        for (const filePath of filePaths) {
          if (filePath.href.endsWith('.gpkg')) {
            filesToProcess.push(filePath.pathname);
          }
        }
      } else if (stat) {
        if (sourcePath.href.endsWith('.gpkg')) {
          filesToProcess.push(sourcePath.pathname);
        }
      }
    }

    if (filesToProcess.length === 0) {
      logger.info('ToParquet:No files to process');
      return;
    }

    await $`mkdir ./parquet`;
    logger.info({ filesToProcess }, 'ToParquet:Processing');
    for (const file of filesToProcess) {
      Q.push(async () => {
        const output = `./parquet/${basename(file, '.gpkg')}.parquet`;
        const command = [
          'ogr2ogr',
          '-f',
          'Parquet',
          output,
          file,
          '-dsco',
          `COMPRESSION=${args.compression}`,
          '-dsco',
          `COMPRESSION_LEVEL=${args.compression_level}`,
          '-dsco',
          `ROW_GROUP_SIZE=${args.row_group_size}`,
        ];
        if (!args.no_sort_by_bbox) {
          command.push('-dsco', 'SORT_BY_BBOX=YES');
        }
        await $`${command}`;
        logger.info({ output }, 'ToParquet:Converted');
      });
    }

    await Q.join().catch((err: unknown) => {
      logger.fatal({ err }, 'ToParquet:Error');
      throw err;
    });
    logger.info('ToParquet:Completed');
  },
});
